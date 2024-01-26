using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Contracts
{
    internal class CosmosDbItem<T>
    {

        public string etag { get; set; }

        public T payload { get; set; }
    }

    internal class Events
    {
        // For cosmos Operations
        public string pk { get; set; }
        public string id { get; set; }

        // Payload
        public string EventName { get; set; }
        public string Scope { get; set; }
        public string Status { get; set; }
        public double ExpirationDuration { get; set; }
        public DateTime FirstSatisfactionTimestamp { get; set; }
        public Dictionary<string, string> Arguments { get; set; }
        public string AssociatedJobId { get; set; }
        public string AssociatedTaskId { get; set; }
        public string AssociatedJobType { get; set; }
        public string AssociatedWorkflow { get; set; }
        public List<Dictionary<string, string>> AdditionalScopes { get; set; }
        public List<Dictionary<string, string>> AssociatedPrerequisites { get; set; }
        public int TimeToSatisfy { get; set; }

        public int SatisfyBy { get; set; }

        public DateTime EventTimestamp { get; set; }
    }
}
