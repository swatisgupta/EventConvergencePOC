using EventConvergencePOCTest.Contracts;
using EventConvergencePOCTest.Src;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Tests
{
    internal class LoadTestEventConvergence
    {
        static int MAX_ITERATIONS = 1000;

        static int MAX_EVENTS = 10;

        static string resultPath = @"C:\Users\swatisinghal\OneDrive - Microsoft\Desktop\EventConvergencePOC\NoAlias";

        static List<string> EventNames;

        static string Scope = "Region.uswest";

        static long jobBase = 2518097332120667215;


        static JobEventMappings jobInput = new JobEventMappings()
        {
            JobId = "_39df583a-48c8-4ce2-a4bb-20f0594ca5ec",
            TaskId = "6E7352D9-57A7-4451-B14B-EEA2D6ADDF74",
            JobType = "AzureFoundation",
            Workflow = "NewAZ",
            AdditionalScopes = new List<Dictionary<string, string>>
                                {
                                    new Dictionary<string, string> { { "Name", "Network.ANPArtifacts.Seed" },
                                        { "Scope", "Cluster.LON22PrdApp10-02" },
                                    },
                                    new Dictionary<string, string> { { "Name", "Network.ANPArtifacts.Seed" },
                                        { "Scope", "Cluster.SN1PrdApp11" },
                                    },
                                },
            Arguments = new Dictionary<string, string> { { "SlbV2BuildPath", "\\\\exme.gbl.eaglex.ic.gov\\Builds\\branches\\git_networking_slb_hotfix_0_11_202_0" },
                                                         { "SlbV2BuildFqbn", "0.11.202.3" }, 
                                                         { "SlbV2MajorVersion", "0" },
                                                         { "SlbV2MinorVersion", "11" },
                                                         { "SlbV2RevisionVersion", "3" },
                                                         { "SlbV2DeploymentToolset", "usegit_networking_slb_hotfix_0_11_202_0=0.11.202.3" } },

            AssociatedPrerequisites = new List<Dictionary<string, string>>
                                    { new Dictionary<string, string> { { "Name", "DCMT.Config" },
                                        { "Scope", "Datacenter.LON22" },
                                        { "IsStaticEntity", "true" } },
                                       new Dictionary<string, string> { { "Name", "Scedo.Artifacts.Generated" },
                                        { "Scope", "DeploymentId.062920191" },
                                        { "IsStaticEntity", "true" } },
                                    },
        };

        // write a main method that will run all the tests
        public static void Main(String[] args)
        {
            LoadTestsWithoutAlias();
            // LoadTestsWithAlias();
        }

        public static void LoadTestsWithAlias()
        {
            resultPath = @"C:\Users\swatisinghal\Documents\EventConvergencePOCNumbers\Alias";

            EventNames = new List<string>() { "ESTS.Milestones.InternallyReady",
                                                              "Milestones.ESTSGeo.InternallyReady",
                                                              "Milestones.ESTSGlobal.InternallyReady",
                                                              "Gtos.Milestones.InternallyReady",
                                                              "Milestones.SqlVM.InternallyReady",
                                                              "AZMilestones.AzNM.InternallyReady",
                                                              "AadddsDpx.Milestones.InternallyReady",
                                                              "OneBranchRelease.Milestones.InternallyReady",
                                                              "Shoebox.Services.InternallyReady",
                                                              "RescuePP.Milestones.InternallyReady" };


            LoadTestRegister();
            
            LoadTestSatisfy();
            
            LoadTestCancel();
            
        }

        public static void LoadTestsWithoutAlias()
        {
            resultPath = @"C:\Users\swatisinghal\Documents\EventConvergencePOCNumbers\NoAlias";

            EventNames = new List<string>() { "Milestones.ESTS.InternallyReady",
                                                              "Milestones.ESTSGeo.InternallyReady",
                                                              "Milestones.ESTSGlobal.InternallyReady",
                                                              "Milestones.Gtos.InternallyReady",
                                                              "Milestones.SqlVM.InternallyReady",
                                                              "AZMilestones.AzNM.InternallyReady",
                                                              "Milestones.AadddsDpx.InternallyReady",
                                                              "Milestones.OneBranchRelease.InternallyReady",
                                                              "Services.Shoebox.InternallyReady",
                                                              "Milestones.RescuePP.InternallyReady" };
           LoadTestRegister();
            
           LoadTestSatisfy();
             
           LoadTestCancel();

           LoadTestGet();

           LoadTestGetSimple();
            
        }

        public static void LoadTestRegister()
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();
            System.IO.Directory.CreateDirectory($"{resultPath}");
            System.IO.File.WriteAllLines($"{resultPath}\\RegisterTimes.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\RegisterRU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\RegisterRUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\RegisterRUEvents.txt", test.GetRUPerDBOPerationEvents());
            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (int j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        jobIndex = jobIndex + 1 % 1000;
                        var jobId = $"{jobBase + jobIndex}{jobInput.JobId}";
                        var taskId = jobInput.TaskId;

                        test.Register(EventNames[j], Scope, jobId, taskId,
                                      jobInput.Arguments,jobInput.AdditionalScopes,jobInput.AssociatedPrerequisites,
                                      jobInput.JobType, jobInput.Workflow);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Register: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Register: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\RegisterTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\RegisterRU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\RegisterRUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\RegisterRUEvents.txt", test.GetRUPerDBOPerationEvents());

            }

        }

        public static void LoadTestGet()
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();
            System.IO.Directory.CreateDirectory($"{resultPath}");

            System.IO.File.WriteAllLines($"{resultPath}\\GetTimes.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\GetRU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\GetRUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\GetRUEvents.txt", test.GetRUPerDBOPerationEvents());

            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        test.GetStatus(EventNames[j], Scope);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Cancel: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Get: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\GetTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\GetRU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\GetRUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\GetRUEvents.txt", test.GetRUPerDBOPerationEvents());
            }
            
        }

        public static void LoadTestGetSimple()
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();
            System.IO.Directory.CreateDirectory($"{resultPath}");

            System.IO.File.WriteAllLines($"{resultPath}\\GetSTimes.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\GetSRU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\GetSRUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\GetSRUEvents.txt", test.GetRUPerDBOPerationEvents());

            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        test.GetStatusSimple(EventNames[j], Scope);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Cancel: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"GetSimple: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\GetSTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\GetSRU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\GetSRUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\GetSRUEvents.txt", test.GetRUPerDBOPerationEvents());
            }

        }

        public static void LoadTestCancel()
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();
            System.IO.Directory.CreateDirectory($"{resultPath}");

            System.IO.File.WriteAllLines($"{resultPath}\\CancelTimes.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\CancelRU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\CancelRUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\CancelRUEvents.txt", test.GetRUPerDBOPerationEvents());

            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        jobIndex = jobIndex + 1 % 1000;
                        var jobId = $"{jobBase + jobIndex}{jobInput.JobId}";
                        var taskId = jobInput.TaskId;
                        test.Cancel(EventNames[j],Scope, jobId, taskId);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Cancel: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Cancel: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\CancelTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\CancelRU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\CancelRUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\CancelRUEvents.txt", test.GetRUPerDBOPerationEvents());


            }
        }

        public static void LoadTestSatisfy()
        {
            EventsService test = new EventsService();
            test.CreateConnection();
            List<string> overall = new List<string>();
            List<string> ru = new List<string>();
            System.IO.Directory.CreateDirectory($"{resultPath}");
            System.IO.File.WriteAllLines($"{resultPath}\\SatisfyTimes.txt", overall);
            System.IO.File.WriteAllLines($"{resultPath}\\SatisfyRU.txt", ru);
            System.IO.File.WriteAllLines($"{resultPath}\\SatisfyRUMappings.txt", test.GetRUPerDBOPerationMappings());
            System.IO.File.WriteAllLines($"{resultPath}\\SatisfyRUEvents.txt", test.GetRUPerDBOPerationEvents());

            Random rnd = new Random(1000);
            int jobIndex = 0;
            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                test.ClearRU();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try 
                    {
                        jobIndex = jobIndex + 1 % 1000;
                        var jobId = $"{jobBase + jobIndex}{jobInput.JobId}";
                        var taskId = jobInput.TaskId;
                        test.Satisfy(EventNames[j], Scope, jobId, taskId,
                                      jobInput.Arguments, jobInput.AdditionalScopes, jobInput.AssociatedPrerequisites,
                                      jobInput.JobType, jobInput.Workflow);
                    }
                    catch (Exception e)
                    {
                         Console.WriteLine($"Satisfy: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{Scope}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Satisfy: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyRU.txt", ru);
                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyRUMappings.txt", test.GetRUPerDBOPerationMappings());
                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyRUEvents.txt", test.GetRUPerDBOPerationEvents());


            }
        }
    }
}
