using EventConvergencePOCTest.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencPOCTest.Contracts
{
    internal class DependencyEntity
    {
        // For cosmos Operations
        public string id { get; set; }
        public string pk { get; set; }

        // ------------------ Payload ---------------
        /// <summary>
        /// The alias name separator in Alias field
        /// </summary>
        private readonly char AliasSeparator = ',';

        /// <summary>
        /// The alias scope separator in Alias field
        /// </summary>
        private readonly char AliasScopeSeparator = ':';

        /// <summary>
        /// The fully qualified entity name
        /// </summary>
        public string EntityName { get; set; }

        /// <summary>
        /// Alternative name of the entity
        /// </summary>
        public string Alias { get; set; }
        
        /// <summary>
        /// Namespace under which the entity belongs
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// List of scopes supported by entity
        /// </summary>
        public List<string> SupportedScopes { get; set; }

        /// <summary>
        /// list of prerequisites/dependencies of the entities
        /// </summary>
        public List<EntityPrerequisite> Prerequisites { get; set; }

        /// <summary>
        /// Record of previously supported scopes
        /// </summary>
        public string PastSupportedScopes { get; set; }

        /// <summary>
        /// Record of previous prerequisites
        /// </summary>
        public string PastPrerequisites { get; set; }

        /// <summary>
        /// Determines whether entity is obsolete, active or pending obsolete
        /// </summary>
        public string State { get; set; }

        /// <summary>
        /// Unique identifier of the application in Service Tree
        /// </summary>
        public string Tenant { get; set; }

        /// <summary>
        /// The last UTC date any modifications were done to the entity.
        /// </summary>
        public DateTime ModifiedDate { get; set; }

        /// <summary>
        /// A true value determines whether the entity is visible publicly.
        /// </summary>
        public bool IsVisible { get; set; }

        /// <summary>
        /// Flag for cross cloud support enabling non public cloud to push
        /// entity on Public cloud
        /// </summary>
        public bool PublicCloudPushEnable { get; set; }

        /// <summary>
        /// Service Tree Id of the Event.
        /// </summary>
        public List<string> ServiceTreeIds { get; set; }

        /// <summary>
        /// EntityLevel Expression for prerequisites
        /// </summary>
        public string Expression { get; set; }

        /// <summary>
        /// Flag to determine if the  record is duplicate for alias
        /// </summary>
        public bool IsDuplicate { get; set; }


        public IEnumerable<string> GetAllEntityNames()
        {
            List<string> allEntityNames = new List<string> { this.EntityName };

            if (!string.IsNullOrWhiteSpace(this.Alias))
            {
                var aliasNames = Alias.Split(AliasSeparator);
                foreach (var aliasName in aliasNames)
                {
                    allEntityNames.Add(aliasName.Split(AliasScopeSeparator)[0]);
                }
            }
            return allEntityNames;
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }

            DependencyEntity other = obj as DependencyEntity;

            if (object.ReferenceEquals(this, other))
            {
                return true;
            }

            return String.Equals(this.ToString(), other?.ToString());
        }

        public static bool operator ==(DependencyEntity entity1, DependencyEntity entity2)
        {
            if (object.ReferenceEquals(entity1, entity2))
            {
                return true;
            }

            return object.ReferenceEquals(entity1, null) ? false : entity1.Equals(entity2);
        }

        public static bool operator !=(DependencyEntity entity1, DependencyEntity entity2)
        {
            return !(entity1 == entity2);
        }

        public override string ToString()
        {
            string toString = $"EntityName: {this.EntityName}, Namespace: {this.Namespace}, Tenant: {this.Tenant}";
            toString += ", Alias:" + (this.Alias == null ? "null" : this.Alias);
            if (this.SupportedScopes != null)
            {
                toString += $", SupportedScopes: [ {String.Join(",", this.SupportedScopes)} ]";
            }

            if (this.Prerequisites != null)
            {
                toString += $", Prerequisites: [ {String.Join(",", this.Prerequisites)} ]";
            }

            return toString.ToLower();
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
