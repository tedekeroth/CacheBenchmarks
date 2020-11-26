using System;
using System.Collections.Generic;
using System.Text;

namespace CouchbaseTests
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class GeoLocation
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public class Address
    {
        public GeoLocation GeoLocation { get; set; }
        public int Id { get; set; }
        public string RoadName { get; set; }
        public string Number { get; set; }
        public string Muni { get; set; }
        public string City { get; set; }
        public string Reference { get; set; }
        public string Comment { get; set; }
        public string Name { get; set; }
        public string Instruction { get; set; }
    }

    public class Node
    {
        public Address Address { get; set; }
        public DateTime Time { get; set; }
        public string Test { get; set; }
        public int Test2 { get; set; }
        public string Test3 { get; set; }
        public string DriverAssistance { get; set; }
        public int? Test4 { get; set; }
        public string Test5 { get; set; }
    }

    public class Root
    {
        public int JobId { get; set; }
        public int TransportId { get; set; }
        public string CustomerName { get; set; }
        public List<Node> Nodes { get; set; }
        public DateTime Time1 { get; set; }
        public string Time2 { get; set; }
        public string Time3 { get; set; }
        public string ServiceFee { get; set; }
        public double ServiceFeeValue { get; set; }
        public string LineInfo { get; set; }
        public string Instruction { get; set; }
        public string PhoneNumber { get; set; }
        public string Test6 { get; set; }
        public string Test7 { get; set; }
        public string Test8 { get; set; }
        public string Test9 { get; set; }
        public bool Test10 { get; set; }
        public string Test11 { get; set; }
        public string Test12 { get; set; }
        public string Test13 { get; set; }
        public DateTime Time4 { get; set; }
        public double Cost { get; set; }
    }


}
