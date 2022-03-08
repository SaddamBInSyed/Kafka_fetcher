using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using SmartVessel.Application.Features.Dashboards.GetData;

namespace SmartVessel.Server.Confluent
{
    public static class Kafka_Fetcher
    {        
        public static List<CamViolation_Lite> RetrieveCameraViolationsFromKafkaByRange(string topic_name, string broker, Timestamp startDate, Timestamp endDate, ref List<CamViolation_Lite> _events)
        {
            //string topic_name = "cam_violations";
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "retrieve_cam_violations",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 15000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                FetchMaxBytes = 15728640,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
            };

            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"configure service : Error: {e.Reason}"))
                        .Build();
            try
            {
                consumer.Subscribe(topic_name);

                List<TopicPartition> topic_partitions = Kafka_Fetcher.GetTopicPartitions(consumerConfig.BootstrapServers, topic_name);

                List<TopicPartitionTimestamp> new_times = new List<TopicPartitionTimestamp>();
                foreach (TopicPartition tp in topic_partitions)
                {
                    new_times.Add(new TopicPartitionTimestamp(tp, startDate));
                }

                List<TopicPartitionOffset> seeked_offsets = consumer.OffsetsForTimes(new_times, TimeSpan.FromSeconds(40));

                foreach (TopicPartitionOffset tpo in seeked_offsets)
                {
                    if (tpo.Offset.Value == -1)
                    {
                        Console.WriteLine($" No vald offset / data found. ");
                        return null;
                    }
                }

                //consume by assigning to all topic partition and specific offsets:
                consumer.Assign(seeked_offsets);

                //List<CamViolation_Lite> _events = new List<CamViolation_Lite>();
                int try_count = 0;
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(2000);
                        // do something with r
                        if (consumeResult?.Message?.Value != null)
                        {
                            try_count = 0;
                            Console.WriteLine($" offset {consumeResult.Offset.Value} time {consumeResult.Message.Timestamp.UtcDateTime}");

                            //check date bw start and end
                            if ((consumeResult.Message.Timestamp.UtcDateTime >= startDate.UtcDateTime) &&
                            (consumeResult.Message.Timestamp.UtcDateTime <= endDate.UtcDateTime))
                            {
                                //end range arrived.
                                Console.WriteLine(" date time is withing range");
                            }
                            else
                            {
                                Console.WriteLine(" date time is NOT withing range");
                                return _events;
                            }

                            CamViolation_Lite eventData = JsonConvert.DeserializeObject<CamViolation_Lite>(
                                consumeResult?.Message?.Value);
                            eventData.Offset = consumeResult.Offset;
                            _events.Add(eventData);
                        }
                        else
                        {
                            Console.WriteLine($" (Db) Waiting for CamViolationsForFilter old events {DateTime.Now}");
                            try_count++;
                            if (try_count > 3)
                            {
                                try_count = 0;
                                break;
                            }
                            continue;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"CamViolationsForFilter Consume error: {e.Error.Reason}");
                        break;
                    }
                }

                return _events;
            }
            catch (Exception ex)
            {
                Console.WriteLine($" Error while retrieivng data {ex.Message}");
            }
            finally
            {
                //clean up    
                consumer?.Close();
                consumer?.Dispose();
            }

            Console.WriteLine($"Consume clean up done:");
            return null;
        }


        /* public static List<HikVizViolation> RetrieveHikVizViolationsFromKafkaByRange(string topic_name, string broker, Timestamp startDate, Timestamp endDate, ref List<HikVizViolation> _events)
        {
            //string topic_name = "hikviz_violations";
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "retrieve_hik_violations",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 15000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                FetchMaxBytes = 15728640,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
            };

            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"configure service : Error: {e.Reason}"))
                        .Build();
            try
            {
                consumer.Subscribe(topic_name);

                List<TopicPartition> topic_partitions = Kafka_Fetcher.GetTopicPartitions(consumerConfig.BootstrapServers, topic_name);

                List<TopicPartitionTimestamp> new_times = new List<TopicPartitionTimestamp>();
                foreach (TopicPartition tp in topic_partitions)
                {
                    new_times.Add(new TopicPartitionTimestamp(tp, startDate));
                }

                List<TopicPartitionOffset> seeked_offsets = consumer.OffsetsForTimes(new_times, TimeSpan.FromSeconds(40));

                foreach (TopicPartitionOffset tpo in seeked_offsets)
                {
                    if (tpo.Offset.Value == -1)
                    {
                        Console.WriteLine($" No vald offset / data found. ");
                        return null;
                    }
                }

                //consume by assigning to all topic partition and specific offsets:
                consumer.Assign(seeked_offsets);

                //List<HikVizViolation> _events = new List<HikVizViolation>();
                int try_count = 0;
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(2000);
                        // do something with r
                        if (consumeResult?.Message?.Value != null)
                        {
                            try_count = 0;
                            Console.WriteLine($" offset {consumeResult.Offset.Value} time {consumeResult.Message.Timestamp.UtcDateTime}");

                            //check date bw start and end
                            if ((consumeResult.Message.Timestamp.UtcDateTime >= startDate.UtcDateTime) &&
                            (consumeResult.Message.Timestamp.UtcDateTime <= endDate.UtcDateTime))
                            {
                                //end range arrived.
                                Console.WriteLine(" date time is withing range");
                            }
                            else
                            {
                                Console.WriteLine(" date time is NOT withing range");
                                return _events;
                            }

                            HikVizViolation eventData = JsonConvert.DeserializeObject<HikVizViolation>(
                                consumeResult?.Message?.Value);

                            _events.Add(eventData);
                        }
                        else
                        {
                            Console.WriteLine($" (Db) Waiting for HikViolationsForFilter old events {DateTime.Now}");
                            try_count++;
                            if (try_count > 3)
                            {
                                try_count = 0;
                                break;
                            }
                            continue;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"HikViolationsForFilter Consume error: {e.Error.Reason}");
                        break;
                    }
                }

                return _events;
            }
            catch (Exception ex)
            {
                Console.WriteLine($" Error while retrieivng data {ex.Message}");
            }
            finally
            {
                //clean up    
                consumer?.Close();
                consumer?.Dispose();
            }

            Console.WriteLine($"Consume clean up done:");
            return null;
        } */


        public static List<InclinationLevel> RetrieveIncLevelsViolationsFromKafkaByRange(string topic_name, string broker, Timestamp startDate, Timestamp endDate)
        {
            //string topic_name = "inclination_levels";
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "retrieve_hik_violations",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 15000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                FetchMaxBytes = 15728640,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
            };

            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"configure service : Error: {e.Reason}"))
                        .Build();
            try
            {
                consumer.Subscribe(topic_name);

                List<TopicPartition> topic_partitions = Kafka_Fetcher.GetTopicPartitions(consumerConfig.BootstrapServers, topic_name);

                List<TopicPartitionTimestamp> new_times = new List<TopicPartitionTimestamp>();
                foreach (TopicPartition tp in topic_partitions)
                {
                    new_times.Add(new TopicPartitionTimestamp(tp, startDate));
                }

                List<TopicPartitionOffset> seeked_offsets = consumer.OffsetsForTimes(new_times, TimeSpan.FromSeconds(40));

                foreach (TopicPartitionOffset tpo in seeked_offsets)
                {
                    if (tpo.Offset.Value == -1)
                    {
                        Console.WriteLine($" No vald offset / data found. ");
                        return null;
                    }
                }

                //consume by assigning to all topic partition and specific offsets:
                consumer.Assign(seeked_offsets);

                List<InclinationLevel> _events = new List<InclinationLevel>();
                int try_count = 0;
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(2000);
                        // do something with r
                        if (consumeResult?.Message?.Value != null)
                        {
                            try_count = 0;
                            Console.WriteLine($" offset {consumeResult.Offset.Value} time {consumeResult.Message.Timestamp.UtcDateTime}");

                            //check date bw start and end
                            if ((consumeResult.Message.Timestamp.UtcDateTime >= startDate.UtcDateTime) &&
                            (consumeResult.Message.Timestamp.UtcDateTime <= endDate.UtcDateTime))
                            {
                                //end range arrived.
                                Console.WriteLine(" date time is withing range");
                            }
                            else
                            {
                                Console.WriteLine(" date time is NOT withing range");
                                return _events;
                            }

                            InclinationLevel eventData = JsonConvert.DeserializeObject<InclinationLevel>(
                                consumeResult?.Message?.Value);

                            _events.Add(eventData);
                        }
                        else
                        {
                            Console.WriteLine($" (Db) Waiting for InclinationLevelsForFilter old events {DateTime.Now}");
                            try_count++;
                            if (try_count > 3)
                            {
                                try_count = 0;
                                break;
                            }
                            continue;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"InclinationLevelsForFilter Consume error: {e.Error.Reason}");
                        break;
                    }
                }

                return _events;
            }
            catch (Exception ex)
            {
                Console.WriteLine($" Error while retrieivng data {ex.Message}");
            }
            finally
            {
                //clean up    
                consumer?.Close();
                consumer?.Dispose();
            }

            Console.WriteLine($"Consume clean up done:");
            return null;
        }


        public static CamViolation RetrieveCameraViolationsFromKafkaByOffset(string topic_name, string broker, long Offset_to_fetch)
        {
            //string topic_name = "cam_violations";
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "retrieve_camImage_violations",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 15000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                FetchMaxBytes = 15728640,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
            };

            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"configure service : Error: {e.Reason}"))
                        .Build();
            try
            {
                consumer.Subscribe(topic_name);

                List<TopicPartition> topic_partitions = Kafka_Fetcher.GetTopicPartitions(consumerConfig.BootstrapServers, topic_name);


                TopicPartitionOffset new_Offset = null;
                Offset _Offset = new(Offset_to_fetch);
                foreach (TopicPartition tp in topic_partitions)
                {
                    new_Offset = new TopicPartitionOffset(tp, _Offset);
                    consumer.Seek(new_Offset);
                }

                CamViolation _events = new CamViolation();
                int try_count = 0;
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(2000);
                        // do something with r
                        if (consumeResult?.Message?.Value != null)
                        {
                            try_count = 0;
                            Console.WriteLine($" offset {consumeResult.Offset.Value} time {consumeResult.Message.Timestamp.UtcDateTime}");

                            _events = JsonConvert.DeserializeObject<CamViolation>(consumeResult?.Message?.Value);
                            return _events;
                        }
                        else
                        {
                            Console.WriteLine($" (Db) Waiting for CamViolations image old events {DateTime.Now}");
                            try_count++;
                            if (try_count > 2)
                            {
                                try_count = 0;
                                break;
                            }
                            continue;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"CamViolations image Consume error: {e.Error.Reason}");
                        break;
                    }
                }

                return _events;
            }
            catch (Exception ex)
            {
                Console.WriteLine($" Error while retrieivng data {ex.Message}");
            }
            finally
            {
                //clean up    
                consumer?.Close();
                consumer?.Dispose();
            }

            Console.WriteLine($"Consume clean up done:");
            return null;
        }



        /* public static HikVizViolation RetrieveHikViolationsFromKafkaByOffset(string topic_name, string broker, long Offset_to_fetch)
        {
            //string topic_name = "cam_violations";
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "retrieve_hikImage_violations",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 15000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                FetchMaxBytes = 15728640,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
            };

            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"configure service : Error: {e.Reason}"))
                        .Build();
            try
            {
                consumer.Subscribe(topic_name);

                List<TopicPartition> topic_partitions = Kafka_Fetcher.GetTopicPartitions(consumerConfig.BootstrapServers, topic_name);


                TopicPartitionOffset new_Offset = null;
                Offset _Offset = new(Offset_to_fetch);
                foreach (TopicPartition tp in topic_partitions)
                {
                    new_Offset = new TopicPartitionOffset(tp, _Offset);
                    consumer.Seek(new_Offset);
                }

                HikVizViolation _events = new HikVizViolation();
                int try_count = 0;
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(2000);
                        // do something with r
                        if (consumeResult?.Message?.Value != null)
                        {
                            try_count = 0;
                            Console.WriteLine($" offset {consumeResult.Offset.Value} time {consumeResult.Message.Timestamp.UtcDateTime}");

                            _events = JsonConvert.DeserializeObject<HikVizViolation>(consumeResult?.Message?.Value);
                            return _events;
                        }
                        else
                        {
                            Console.WriteLine($" (Db) Waiting for HikVizViolation image old events {DateTime.Now}");
                            try_count++;
                            if (try_count > 2)
                            {
                                try_count = 0;
                                break;
                            }
                            continue;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"HikVizViolation image Consume error: {e.Error.Reason}");
                        break;
                    }
                }

                return _events;
            }
            catch (Exception ex)
            {
                Console.WriteLine($" Error while retrieivng data {ex.Message}");
            }
            finally
            {
                //clean up    
                consumer?.Close();
                consumer?.Dispose();
            }

            Console.WriteLine($"Consume clean up done:");
            return null;
        } */



        /// <summary>
        /// get the list of topic partitions:
        /// </summary>
        /// <param name="bootstrapServers"></param>
        /// <param name="topicValue"></param>
        /// <returns></returns>
        private static List<TopicPartition> GetTopicPartitions(string bootstrapServers, string topicValue)
        {
            var tp = new List<TopicPartition>();
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                meta.Topics.ForEach(topic =>
                {
                    if (topic.Topic == topicValue)
                    {
                        foreach (PartitionMetadata partition in topic.Partitions)
                        {
                            tp.Add(new TopicPartition(topic.Topic, partition.PartitionId));
                        }
                    }
                });
            }
            return tp;
        }
    }
}