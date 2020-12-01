using Aerospike.Client;
using Couchbase;
using Couchbase.Analytics;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Query;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CouchbaseTests
{
    public class CouchbaseTests
    {
        private string jsonFile = Environment.CurrentDirectory + @"\job.json";

        private JObject _baseJsonObject = null;
        private List<Root> pocoObjects = new List<Root>();
        private List<string> firstnames = new List<string>();
        private List<string> lastnames = new List<string>();
        private Random rand = new Random();
        private ICluster cluster;
        private RedisManagerPool redisManager;
        private AerospikeClient aeroClient;

        public CouchbaseTests(string serviceStackLicense)
        { 
            Licensing.RegisterLicense(serviceStackLicense); // ServiceStack licensing, if no license, dont test using Redis/SS, or replace lib
            firstnames.AddRange(File.ReadAllText(Environment.CurrentDirectory + @"\firstnames.txt").Split(Environment.NewLine));
            lastnames.AddRange(File.ReadAllText(Environment.CurrentDirectory + @"\lastnames.txt").Split(Environment.NewLine));
        }

        public async Task Init()
        {
            try { aeroClient = new AerospikeClient("127.0.0.1", 3000); } catch { }

            try { cluster = await Couchbase.Cluster.ConnectAsync("couchbase://localhost", "root", "root"); } catch { }
            
            try { redisManager = new RedisManagerPool("localhost:6379"); } catch { }

            _baseJsonObject = JObject.Parse(File.ReadAllText(jsonFile));
        }

        [Flags]
        public enum Database
        {
            None = 0,
            Redis = 1,
            Couchbase = 2,
            MySql = 4,
            Aerospike = 8,
            RediSql = 16
        }

        public async Task CreateJobs(int nbr, Database databases, bool useStronglyTyped = false)
        {
            // pocoObjects.Add(JsonConvert.DeserializeObject<Root>(temp.ToString())); // added option to use strongly typed stuff

            Stopwatch sw = new Stopwatch();
            if (databases.HasFlag(Database.Couchbase))
            {
                // TODO You need to setup Couchbase with the buckets etc as listed here
                IBucket bucket = await cluster.BucketAsync("myBucket");
                IScope scope = bucket.Scope("myScope");
                var collection = scope.Collection("myCollection");

                // avoid measuring lazy loading:
                JObject t = JObject.FromObject(this._baseJsonObject);
                t["JobId"] = 0;
                t["CustomerName"] = $"{firstnames[rand.Next(0, firstnames.Count - 1)]} {lastnames[rand.Next(0, lastnames.Count - 1)]}";
                await collection.InsertAsync("0", t);
                await collection.RemoveAsync("0");

                // List<Task> inserTasks = new List<Task>();
                sw.Start();
                if (useStronglyTyped)
                {
                    for (int i = 0; i < nbr; i++)
                    {
                        JObject tempObject = SetTempValue(_baseJsonObject, i);
                        await collection.InsertAsync(i.ToString(), JsonConvert.DeserializeObject<Root>(tempObject.ToString()));
                    }
                }
                else
                {
                    for (int i = 0; i < nbr; i++)
                    {
                        JObject tempObject = SetTempValue(_baseJsonObject, i);
                        await collection.InsertAsync(i.ToString(), tempObject);
                    }
                }
                // await Task.WhenAll(inserTasks);
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to Couchbase took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }

            if (databases.HasFlag(Database.Redis))
            {
                sw.Restart();
                using (var client = redisManager.GetClient())
                {
                    // no concepts of strongly typed in redis...
                    for (int i = 0; i < nbr; i++)
                    {
                        JObject tempObject = SetTempValue(_baseJsonObject, i);
                        client.Set($"jobId:{tempObject.GetValue("JobId")}", tempObject.ToString());
                    }
                }
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to Redis took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }

            if (databases.HasFlag(Database.MySql)) // file 'mysql-table-sql' has table def
            {
                MySqlConnection mySqlConnection = new MySqlConnection("Server=localhost;Database=test;port=3306;User Id=root;password=root;"); // TODO replace user / pass
                mySqlConnection.Open();
                sw.Restart();
                for (int i = 0; i < nbr; i++)
                {
                    JObject tempObject = SetTempValue(_baseJsonObject, i);
                    MySqlCommand cmd = new MySqlCommand($"INSERT INTO test (id, data) VALUES ('{tempObject.GetValue("JobId")}', @data)", mySqlConnection);
                    cmd.Parameters.AddWithValue("@data", tempObject.ToString());
                    cmd.ExecuteNonQuery();
                }
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to MySql took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }

            if (databases.HasFlag(Database.Aerospike))
            {
                /* namespace = database
                 * sets = tables
                 * records = rows
                 * bins = columns */

                sw.Restart();
                // no concept of strongly typed
                for (int i = 0; i < nbr; i++)
                {
                    JObject temp = SetTempValue(_baseJsonObject, i);
                    aeroClient.Put(null, new Key("test", "cache", temp.GetValue("JobId").ToString()), new Bin[]
                    {
                        new Bin("Id", temp.GetValue("JobId").ToString()),
                        new Bin("Data", temp.ToString())
                    });
                }
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to Aerospike took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }

            if (databases.HasFlag(Database.RediSql))
             {
                var dbName = "db";
                using (var client = redisManager.GetClient())
                {
                    client.Custom($"DEL", dbName);
                    client.Custom($"REDISQL.CREATE_DB", dbName);
                    client.Custom($"REDISQL.EXEC", dbName, $"CREATE TABLE jobcache (Id INT, Data TEXT, timestamp TEXT)");
                    client.Custom($"REDISQL.EXEC", dbName, $"CREATE INDEX jobid_idx ON jobcache (Id)");
                    
                }
                List<long> times = new List<long>();
                sw.Restart();
                using (var client = redisManager.GetClient())
                {
                    Stopwatch sw2 = new Stopwatch();
                    
                    // no concepts of strongly typed in redis...
                    for (int i = 0; i < nbr; i++)
                    {
                        JObject tempObject = SetTempValue(_baseJsonObject, i);
                        sw2.Restart();
                        RediSqlCommand(client, dbName, $"INSERT INTO jobcache VALUES ({tempObject.GetValue("JobId")}, '{tempObject.ToString(Formatting.Indented)}', datetime('now'))");
                        sw2.Stop();
                        times.Add(sw2.ElapsedTicks);

                        if (i % 5000 == 0)
                        {
                            Console.WriteLine($"i={i}: " + times[i]);
                        }
                    }
                }
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to RediSql took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }
        }

        /// <summary>
        /// This will NOT work with Parallel work, async/await Task.WhenAll, since it changes the same object all the time!!!
        /// </summary>
        /// <param name="jObject"></param>
        /// <param name="i"></param>
        /// <returns></returns>
        private JObject SetTempValue(JObject jObject, int i)
        {
            jObject["JobId"] = i;
            jObject["CustomerName"] = $"{firstnames[rand.Next(0, firstnames.Count - 1)]} {lastnames[rand.Next(0, lastnames.Count - 1)]}";
            return jObject;
        }

        private RedisText RediSqlCommand(IRedisClient client, string db, string sqlCommand)
        {
            string command = $"REDISQL.EXEC";

            List<string> finalColumnNames = new List<string>();
            if (sqlCommand.Trim().StartsWith("SELECT"))
            {
                var from = sqlCommand.IndexOf("SELECT ") + "SELECT ".Length;
                var to = sqlCommand.IndexOf(" FROM");
                var selectedColumns = sqlCommand[from..to];

                if (selectedColumns.Equals("*"))
                {
                    // Find all cols
                    RedisText sqlitemaster = client.Custom(command, db, "select * from sqlite_master;");
                }
                else
                {
                    string[] cols = selectedColumns.Split(",");
                    for (int i = 0; i < cols.Length; i++)
                    {
                        cols[i] = cols[i].Trim();
                    }

                    foreach(string col in cols)
                    {
                        string colName = null;
                        if (col.Contains(" as "))
                        {
                            var i1 = col.IndexOf(" as ") + " AS ".Length;
                            colName = col[i1..];
                        }
                        else
                        {
                            colName = col;
                        }
                        finalColumnNames.Add(colName);
                    }
                }
            }

            
            RedisText rt = client.Custom(command, db,  sqlCommand);
            return rt;
        }

        public async Task SelectRandomJobs(int nbr, Database databases)
        {
            Random r = new Random();
            if (databases.HasFlag(Database.Couchbase))
            {
                var options = new QueryOptions().Metrics(true);
                IBucket bucket = await cluster.BucketAsync("halo");
                IScope scope = bucket.Scope("myScope");
                var collection = scope.Collection("myCollecton");

                int lim = 10;
                for (int q = 0; q < lim; q++)
                {
                    List<Task> tasks = new List<Task>();
                    Stopwatch sw = Stopwatch.StartNew();
                    for (int i = 0; i < nbr; i++)
                    {
                        string query = $"SELECT * FROM jobcache WHERE JobId = {r.Next(1, 100000)}";
                        //tasks.Add(scope.QueryAsync<dynamic>(query));
                        var queryResult = await scope.QueryAsync<dynamic>(query, options);

                        //string key = $"{r.Next(1, 100000)}";
                        //var result = await collection.GetAsync(key, options: new GetOptions().Transcoder(new LegacyTranscoder()));
                        //var content = result.ContentAs<string>();
                    }
                    // await Task.WhenAll(tasks);
                    sw.Stop();
                    Console.WriteLine($"Couchbase Q: {q}\t{sw.ElapsedMilliseconds}");
                }
            }


            if (databases.HasFlag(Database.Redis))
            {
                for (int q = 0; q < 10; q++)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    using (var client = redisManager.GetClient())
                    {
                        for (int i = 0; i < nbr; i++)
                        {
                            client.Get<string>($"jobId:{r.Next(1, 100000)}");
                        }
                    }
                    sw.Stop();
                    Console.WriteLine($"Redis Q: {q}\t{sw.ElapsedMilliseconds}");
                }
            }

            if (databases.HasFlag(Database.MySql))
            {
                MySqlConnection mySqlConnection = new MySqlConnection("Server=localhost;Database=test;port=3306;User Id=root;password=root;");
                mySqlConnection.Open();

                for (int q = 0; q < 10; q++)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    for (int i = 0; i < nbr; i++)
                    {
                        MySqlCommand cmd = new MySqlCommand($"SELECT data FROM test WHERE Id='{r.Next(1, 100000)}'", mySqlConnection);
                        using MySqlDataReader rdr = cmd.ExecuteReader();

                        while (rdr.Read())
                        {
                        }
                    }
                    sw.Stop();
                    Console.WriteLine($"MySql Q: {q} \t{sw.ElapsedMilliseconds} ms");
                    sw.Reset();
                }
            }


            if (databases.HasFlag(Database.Aerospike))
            {
                for (int q = 0; q < 10; q++)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    for (int i = 0; i < nbr; i++)
                    {
                        Record record = aeroClient.Get(null, new Key("test", "cache", r.Next(1, 100000).ToString()), "Data");
                    }
                    sw.Stop();
                    Console.WriteLine($"Aerospike Q: {q} \t{sw.ElapsedMilliseconds} ms");
                    sw.Reset();
                }
            }

            if (databases.HasFlag(Database.RediSql))
            {
                for (int q = 0; q < 10; q++)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    using (var client = redisManager.GetClient())
                    {
                        RedisText redisText = client.Custom($"type", "db");

                        for (int i = 0; i < nbr; i++)
                        {
                            //RedisText t = RediSqlCommand(client, "db", $"SELECT Id, Data, datetime(timestamp) as dt FROM jobcache WHERE Id={r.Next(1, 100000)} OR Id={r.Next(1, 100000)}");
                            RedisText t = RediSqlCommand(client, "db", $"SELECT * FROM jobcache WHERE Id={r.Next(1, 100000)} OR Id={r.Next(1, 100000)}");
                            var keys = t.GetResults();
                        }
                    }
                    sw.Stop();
                    Console.WriteLine($"RediSql Q: {q}\t{sw.ElapsedMilliseconds}");
                }
            }
        }
    }
}
