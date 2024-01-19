using EventConvergencPOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Azure.Core.HttpHeader;

namespace EventConvergencePOCTest.Src
{
    public class CatalogDataSource
    {
        CosmosClient cosmosClient;
        Database CatalogDatabase;
        // will use event catalog data source
        Container CatalogContainer;
        string CatalogDatabaseId = "EventCatalog";
        string CatalogContainerId = "DependencyEntities";

        double RCCatalog = 0;

        public CatalogDataSource(CosmosClient cosmosClient)
        {
            this.cosmosClient = cosmosClient;
        }


        public void GetCatalogContainer()
        {
            if (CatalogContainer != null)
            {
                return;
            }

            this.CatalogDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(CatalogDatabaseId).GetAwaiter().GetResult()?.Database;
            var partitionKey = "pk";
            var containerProperties = new ContainerProperties(CatalogContainerId, $"/{partitionKey}");
            var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);
            this.CatalogContainer = CatalogDatabase.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughputProperties).GetAwaiter().GetResult()?.Container;
        }

        private void resetTimings()
        {
            RCCatalog = 0;
        }

        public List<string> GetCatalogEntity(string EventName, out string EntityNewName)
        {
            resetTimings();

            List<string> altNames = new List<string> { EventName };

            // Get catalog data for eventname and get all alias names
            try
            {
                var entry = CatalogContainer.ReadItemAsync<DependencyEntity>(EventName.ToLower(), new PartitionKey(EventName.Split('.')[0].ToLower())).GetAwaiter().GetResult();
                // find event with event name and alias name in EventsContainer

                EntityNewName = null;
                RCCatalog += entry.RequestCharge;

                if (entry?.Resource != null)
                {
                    EntityNewName = entry.Resource?.EntityName;
                    IEnumerable<string> differenceQuery = entry.Resource.GetAllEntityNames().Except(altNames);
                    altNames.AddRange(differenceQuery); 
                }
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                EntityNewName = null;
            }
            catch (Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
            return altNames;
        }

        public double GetCatalogRU()
        {
            return RCCatalog;
        }
    }
}
