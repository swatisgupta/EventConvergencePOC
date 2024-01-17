using EventConvergencePOCTest.Src;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencePOCTest.Tests
{
    internal class LoadTestEventConvergence
    {
        static int MAX_ITERATIONS = 1000;

        static int MAX_EVENTS = 10;

        static string resultPath = @"C:\Users\swatisinghal\OneDrive - Microsoft\Desktop\EventConvergencePOC\NoAlias";

        static List<string> EventNames;

        static string ScopeType = "Regional";

        static List<string> ScopeVal = new List<string> { "S1", "S2", "S3", "S4", "S5",
                                                          "S6", "S7", "S8", "S9", "S10" };
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
            Random rnd = new Random(1000);
            for (int i = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                for (int j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        test.Register(EventNames[j], $"{ScopeType}.{ScopeVal[j]}", $"J{rnd.Next(1, 1000)}", "T1");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Register: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{ScopeVal[j]}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Register: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\RegisterTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\RegisterRU.txt", ru);
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
            Random rnd = new Random(1000);
            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try
                    {
                        test.Cancel(EventNames[j], $"{ScopeType}.{ScopeVal[j]}", $"J{rnd.Next(1, 1000)}", "T1");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Cancel: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{ScopeVal[j]}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Cancel: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\CancelTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\CancelRU.txt", ru);

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
            Random rnd = new Random(1000);

            for (int i = 0, j = 0; i < MAX_ITERATIONS; i++)
            {
                overall.Clear();
                ru.Clear();
                for (j = 0; j < MAX_EVENTS; j++)
                {
                    try 
                    {
                        test.Satisfy(EventNames[j], $"{ScopeType}.{ScopeVal[j]}", $"J{rnd.Next(1, 1000)}", "T1");
                    }
                    catch (Exception e)
                    {
                         Console.WriteLine($"Satisfy: Exception at iteration {i * MAX_EVENTS + j} ({EventNames[j]}-{ScopeVal[j]}) Message: {e.Message}");
                    }
                    overall.Add(test.GetExecutionTimes());
                    ru.Add(test.GetRUConsumption());
                }
                Console.WriteLine($"Satisfy: Iteration {i + 1} complete");

                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyTimes.txt", overall);
                System.IO.File.AppendAllLines($"{resultPath}\\SatisfyRU.txt", ru);

            }
        }
    }
}
