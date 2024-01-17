using EventConvergencePOCTest.Contracts;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Src
{
    internal class JobEventMappingsDataSource
    {
        CosmosClient cosmosClient;
        Dictionary<string, Container> JobEventContainers;
        List<string> JobEventsContainerId = new List<string> { "JobEventsMapping" };
        Database EventDatabase;
        string databaseId = "EventsTest";
        List<string> EventsContainerId = new List<string> { "Events" };

        double RCMappings = 0;

        public JobEventMappingsDataSource (CosmosClient cosmosClient)
        {
            this.cosmosClient = cosmosClient;
        }

        private void ResetRU()
        {
            RCMappings = 0;
        }
        public void GetJobEventContainers()
        {

            this.JobEventContainers = new Dictionary<string, Container>();

            EventDatabase = cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId).GetAwaiter().GetResult()?.Database;

            FeedIterator<ContainerProperties> iterator = EventDatabase.GetContainerQueryIterator<ContainerProperties>();
            FeedResponse<ContainerProperties> containers = iterator.ReadNextAsync().ConfigureAwait(false).GetAwaiter().GetResult();


            for(int i=0; i< containers.Count; i++)
            {
                var containerId = containers.ElementAt(i).Id;

                if ( EventsContainerId.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    continue;
                }

                if(this.JobEventsContainerId.Contains(containerId, StringComparer.OrdinalIgnoreCase))
                {
                    this.JobEventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
                else
                {
                    this.JobEventsContainerId.Append(containerId);
                    this.JobEventContainers.Add(containerId, EventDatabase.GetContainer(containerId));
                }
            }

            foreach (var containerId in JobEventsContainerId)
            {
                if (JobEventContainers.ContainsKey(containerId))
                {
                    continue;
                }

                List<string> subPartitionKeyPaths = new List<string> {
                                                    "/PublishingEvent",
                                                    "/Scope" };
                string partitionKey = "/pk";
                var containerProperties = new ContainerProperties(containerId, partitionKey);
                var throughputProperties = ThroughputProperties.CreateAutoscaleThroughput(1000);
                JobEventContainers[containerId] = EventDatabase.CreateContainerIfNotExistsAsync(
                    containerProperties,
                    throughputProperties).GetAwaiter().GetResult()?.Container;

            }
        }

        public void UpdateJobEventMappings(JobEventMappings newState)
        {
            ResetRU();
            try
            {
                newState.pk = $"{newState.PublishingEvent}-{newState.Scope}".ToLower();
                newState.id = $"{newState.JobId}-{newState.TaskId}".ToLower();
                var response = JobEventContainers[JobEventsContainerId[0]].UpsertItemAsync<JobEventMappings>(newState).GetAwaiter().GetResult();
                RCMappings += response.RequestCharge;
            }
            catch(Exception ex)
            {
                // Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        public List<JobEventMappings> GetAllJobsEmitEvent(List<string> EventNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            List<JobEventMappings> resultsList = new List<JobEventMappings>();
            ResetRU();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.Status = 'Satisfied' AND c.BindingState = 'Active'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.RCMappings += results.RequestCharge;
                    if (results != null)
                    {
                        resultsList.AddRange(results.ToList());
                    }

                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return resultsList;
        }

        public List<JobEventMappings> GetAllJobsRegisterEvent(List<string> EventNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            List<JobEventMappings> resultsList = new List<JobEventMappings>();
            
            ResetRU();
            foreach (var name in EventNames)
            {

                try
                {
                    var query = $"SELECT * FROM c WHERE c.PublishingEvent = '{name}' AND c.Scope = '{scope}' AND c.Status = 'Registered' AND c.BindingState = 'Active'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();
                    this.RCMappings += results.RequestCharge;
                    if (results != null)
                    {
                        resultsList.AddRange(results.ToList());
                    }

                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    continue;
                }
            }
            return resultsList;
        }

        public JobEventMappings GetEventForJob(string jobId, string taskId, List<string> altNames, string scope)
        {
            var JobEventMappingsContainer = JobEventContainers[JobEventsContainerId[0]];
            ResetRU();
            // foreach (var name in altNames)
            // {
            //     ItemResponse<JobEventMappings> response = null;

                try
                {
                    var query = $"SELECT * FROM c WHERE c.JobId = '{jobId}' AND c.TaskId = '{taskId}'";
                    var queryDefinition = new QueryDefinition(query);
                    var queryIterator = JobEventMappingsContainer.GetItemQueryIterator<JobEventMappings>(queryDefinition);
                    var results = queryIterator.ReadNextAsync().GetAwaiter().GetResult();

                    if(results != null)
                    {
                        foreach(var result in results)
                        {
                            if(altNames.Contains(result.PublishingEvent) && scope.Equals(result.Scope))
                            {
                                return result;
                            }

                        }
                    }
/*
                    string id = $"{jobId}-{taskId}".ToLower();
                    string pK = $"{name}-{scope}".ToLower();
                    response = JobEventMappingsContainer.ReadItemAsync<JobEventMappings>(id, new PartitionKey(pK)).GetAwaiter().GetResult();
                    RCMappings += response.RequestCharge;
*/
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                    // continue;
                }
            /*
                if (response.Resource != null && response.Resource.BindingState == "Active")
                {
                    // this.serverSideQueryTimeMappings += entry.Diagnostics.GetClientElapsedTime().TotalMilliseconds;
                    // this.serverSideQueryRCMappings += entry.RequestCharge;
                    return response.Resource;
                }

            }
            */
            return null;
        }

        public double GetJobEventMappingsRU()
        {
            return RCMappings;
        }
    }
}
