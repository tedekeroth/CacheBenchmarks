using Aerospike.Client;
using Couchbase;
using Couchbase.Analytics;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Query;
using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;
using ServiceStack;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CouchbaseTests
{
    public class CouchbaseTests
    {
        private string jsonFile = Environment.CurrentDirectory + @"\job.json";

        private JObject _baseJsonObject = null;
        private List<JObject> jsonObjects = new List<JObject>();
        private List<string> firstnames = new List<string>();
        private List<string> lastnames = new List<string>();
        private Random rand = new Random();
        private ICluster cluster;
        private RedisManagerPool redisManager;
        private AerospikeClient aeroClient;

        public CouchbaseTests(string serviceStackLicense)
        { 
            Licensing.RegisterLicense(serviceStackLicense); // ServiceStack licensing, if no license, dont test using Redis/SS, or replace lib
            jsonObjects = new List<JObject>();
            firstnames.AddRange(File.ReadAllText(Environment.CurrentDirectory + @"\firstnames.txt").Split(Environment.NewLine));
            lastnames.AddRange(File.ReadAllText(Environment.CurrentDirectory + @"\lastnames.txt").Split(Environment.NewLine));
        }

        public async Task Init()
        {
            aeroClient = new AerospikeClient("127.0.0.1", 3000);
            cluster = await Couchbase.Cluster.ConnectAsync("couchbase://localhost", "root", "root");
            redisManager = new RedisManagerPool("localhost:6379");
            _baseJsonObject = JObject.Parse(File.ReadAllText(jsonFile));
        }

        [Flags]
        public enum Database
        {
            None = 0,
            Redis = 1,
            Couchbase = 2,
            MySql = 4,
            Aerospike = 8
        }

        public async Task CreateJobs(int nbr, Database databases)
        {
            jsonObjects.Clear();
            for (int i = 0; i < nbr; i++)
            {
                JObject temp = JObject.FromObject(_baseJsonObject);
                temp["JobId"] = i;
                temp["CustomerName"] = $"{firstnames[rand.Next(0, firstnames.Count - 1)]} {lastnames[rand.Next(0, lastnames.Count - 1)]}";
                jsonObjects.Add(temp);
            }

            Stopwatch sw = new Stopwatch();
            if (databases.HasFlag(Database.Couchbase))
            {
                // TODO You need to setup Couchbase with the buckets etc as listed here
                IBucket bucket = await cluster.BucketAsync("myBucket");
                IScope scope = bucket.Scope("myScope");
                var collection = scope.Collection("myCollection");

                List<Task> inserTasks = new List<Task>();
                sw.Start();
                foreach (JObject temp in jsonObjects)
                {
                    inserTasks.Add(collection.InsertAsync(temp.GetValue("JobId").ToString(), temp));
                }
                await Task.WhenAll(inserTasks);
                sw.Stop();
                Console.WriteLine($"Adding {nbr} to Couchbase took {sw.ElapsedMilliseconds} ms");
                sw.Reset();
            }

            if (databases.HasFlag(Database.Redis))
            {
                sw.Restart();
                using (var client = redisManager.GetClient())
                {
                    foreach (JObject temp in jsonObjects)
                    {
                        client.Set($"jobId:{temp.GetValue("JobId")}", temp.ToString());
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
                foreach (JObject temp in jsonObjects)
                {
                    MySqlCommand cmd = new MySqlCommand($"INSERT INTO test (id, data) VALUES ('{temp.GetValue("JobId")}', @data)", mySqlConnection);
                    cmd.Parameters.AddWithValue("@data", temp.ToString());
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
                foreach (JObject temp in jsonObjects)
                {
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
        }
    }
}
