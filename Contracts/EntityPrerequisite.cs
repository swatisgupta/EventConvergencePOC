using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Contracts
{
    internal class EntityPrerequisite
    {
        /// <summary>
        /// Fully qualified name of the prerequisite
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Because an entity can contain multiple scopes,
        /// the required scope of the pre-requisite is mandatory
        /// </summary>
        public string Scope { get; set; }

        /// <summary>
        /// Optional expression to determine context in which the prerequisite apply
        /// </summary>
        public string Expression { get; set; }

        public override string ToString()
        {
            var convertedStr = $"{{Name:{this.Name}, Scope:{this.Scope}";
            if (!String.IsNullOrWhiteSpace(this.Expression))
            {
                convertedStr += $", Expression:{this.Expression}";
            }
            convertedStr += "}";
            return convertedStr;
        }
    }
}
