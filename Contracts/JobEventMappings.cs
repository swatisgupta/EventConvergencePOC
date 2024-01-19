using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Contracts
{
    internal class JobEventMappings
    {
        // For Cosmos Operations
        public string id { get; set; }
        public string pk1 { get; set; }
        public string pk2 { get; set; }

        // ------- Payload ------------------------------------
        public string JobId { get; set; }
        public string TaskId { get; set; }
        public string PublishingEvent { get; set; }
        public string Scope { get; set; }
        public string Status { get; set; }
        public string BindingState { get; set; }
        public string Workflow { get; set; }
        public string JobType { get; set; }
        public Dictionary<string, string> Arguments { get; set; }
        public List<Dictionary<string, string>> AdditionalScopes { get; set; }
        public List<Dictionary<string, string>> AssociatedPrerequisites { get; set; }
        public int TimeToSatisfy { get; set; }
        public DateTime EventTimestamp { get; set; }
    }
}
