using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Azure.Cosmos;
using Company.models;
using System.Text;

namespace Company.Function
{
    public static class HttpTrigger1
    {
        [FunctionName("HttpTrigger1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");


            try
            {
                using CosmosClient cosmosClient = new(
                    accountEndpoint: "https://sql-etl.documents.azure.com:443/",
                    authKeyOrResourceToken: "0JqicxXfrc6HDNfTWSWhOW9X9mreKN73qL1hBYwo4IbGZB23Xwu1NTROAH2CHj6eezPa2MoSYqpYACDbMohebA=="
                );

                var container = cosmosClient.GetContainer("tracking-db", "tracking");
                ReplicaSet readItem = await container.ReadItemAsync<ReplicaSet>(
                    id: "11f76159-4b83-43dd-9eb9-282fd906219d",
                    partitionKey: new PartitionKey("11f76159-4b83-43dd-9eb9-282fd906219d")
                );
                

                SqlConnectionStringBuilder builderForSource = new
                SqlConnectionStringBuilder();

                builderForSource.DataSource = "sql-source.eastus.cloudapp.azure.com";
                builderForSource.UserID = "mgarner";
                builderForSource.Password = "M!2jf@nlubgm";
                builderForSource.InitialCatalog = "wwi-1";
                builderForSource.Authentication = SqlAuthenticationMethod.SqlPassword;
                builderForSource.TrustServerCertificate = true;

                SqlConnectionStringBuilder builderForDest = new
                SqlConnectionStringBuilder();

                builderForDest.DataSource = "sql-etl-dest.database.windows.net";
                builderForDest.UserID = "mgarner";
                builderForDest.Password = "M!2jf@nlubgm";
                builderForDest.InitialCatalog = "wwi-1-dest";
                builderForDest.Authentication = SqlAuthenticationMethod.SqlPassword;
                builderForDest.TrustServerCertificate = true;

                using (SqlConnection connectionForSource = new SqlConnection(builderForSource.ConnectionString))
                {

                    connectionForSource.Open();
                    if (readItem.isInitialized)
                    {
                        String sql = $"SELECT CHANGE_TRACKING_CURRENT_VERSION()";
                        long newLocation = 0;
                        using (SqlCommand command = new SqlCommand(sql, connectionForSource))
                        {
                            newLocation = (long)command.ExecuteScalar();
                        }

                        sql  =  "SELECT CT.SYS_CHANGE_OPERATION, CT.CityID, C.CityName, C.StateProvinceID, C.LatestRecordedPopulation, C.LastEditedBy " +
                                "FROM Application.Cities AS C " +
                               $"RIGHT OUTER JOIN CHANGETABLE(CHANGES Application.Cities, {readItem.syncLocation}) AS CT "+
                                "ON C.CityID = CT.CityID;";

                        using (SqlCommand command = new SqlCommand(sql, connectionForSource))
                        {
                            SqlDataReader reader = command.ExecuteReader();

                            if (reader.HasRows)
                            {
                                using (SqlConnection connectionForDest = new SqlConnection(builderForDest.ConnectionString))
                                {
                                    connectionForDest.Open();
                                    while (reader.Read())
                                    {
                                        string sqlAction = string.Empty;

                                        //DELETE
                                        if (reader.GetString(reader.GetOrdinal("SYS_CHANGE_OPERATION")) == "D")
                                        {
                                            sqlAction = $"DELETE FROM Application.Cities ";
                                            sqlAction += $"WHERE CityID = {reader.GetInt32(reader.GetOrdinal("CityID")).ToString()}";
                                        }

                                        // INSERT
                                        if (reader.GetString(reader.GetOrdinal("SYS_CHANGE_OPERATION")) == "I")
                                        {
                                            sqlAction = $"INSERT INTO Application.Cities (CityID, CityName, StateProvinceID, LatestRecordedPopulation, LastEditedBy) VALUES (";

                                            if (!reader.IsDBNull(reader.GetOrdinal("CityID")))
                                            {
                                                sqlAction += $"{reader.GetInt32(reader.GetOrdinal("CityID"))}, ";
                                            }                      
                                            else
                                            {
                                                sqlAction += "NULL, ";
                                            }              
                                            if (!reader.IsDBNull(reader.GetOrdinal("CityName")))
                                            {
                                                sqlAction += $"'{reader.GetString(reader.GetOrdinal("CityName")).Replace("'", "''")}', ";
                                            }
                                            else
                                            {
                                                sqlAction += "NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("StateProvinceID")))
                                            {
                                                sqlAction += $"{reader.GetInt32(reader.GetOrdinal("StateProvinceID"))}, ";
                                            }
                                            else
                                            {
                                                sqlAction += "NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("LatestRecordedPopulation")))
                                            {
                                                sqlAction += $"{reader.GetInt64(reader.GetOrdinal("LatestRecordedPopulation"))}, ";
                                            }
                                            else
                                            {
                                                sqlAction += "NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("LastEditedBy")))
                                            {
                                                sqlAction += $"{reader.GetInt32(reader.GetOrdinal("LastEditedBy"))} ";
                                            }
                                            else
                                            {
                                                sqlAction += "NULL";
                                            }   
                                            sqlAction += $")";
                                        }

                                        //UPDATES
                                        if (reader.GetString(reader.GetOrdinal("SYS_CHANGE_OPERATION")) == "U")
                                        {
                                            sqlAction = $"UPDATE Application.Cities SET ";
                                            
                                            if (!reader.IsDBNull(reader.GetOrdinal("CityName")))
                                            {
                                                sqlAction += $"CityName = '{reader.GetString(reader.GetOrdinal("CityName")).Replace("'", "''")}', ";
                                            }
                                            else
                                            {
                                                sqlAction += "CityName = NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("StateProvinceID")))
                                            {
                                                sqlAction += $"StateProvinceID = {reader.GetInt32(reader.GetOrdinal("StateProvinceID"))}, ";
                                            }
                                            else
                                            {
                                                sqlAction += "StateProvinceID = NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("LatestRecordedPopulation")))
                                            {
                                                sqlAction += $"LatestRecordedPopulation = {reader.GetInt64(reader.GetOrdinal("LatestRecordedPopulation"))}, ";
                                            }
                                            else
                                            {
                                                sqlAction += "LatestRecordedPopulation = NULL, ";
                                            }   
                                            if (!reader.IsDBNull(reader.GetOrdinal("LastEditedBy")))
                                            {
                                                sqlAction += $"LastEditedBy = {reader.GetInt32(reader.GetOrdinal("LastEditedBy"))} ";
                                            }
                                            else
                                            {
                                                sqlAction += "LastEditedBy = NULL";
                                            }   
                                            sqlAction += $"WHERE CityID = {reader.GetInt32(reader.GetOrdinal("CityID")).ToString()}";
                                        }

                                        using (SqlCommand commandAction = new SqlCommand(sqlAction, connectionForDest))
                                        {
                                            commandAction.ExecuteNonQuery();
                                        }

                                    }
                                }
                                
                            }
                        }
                        //update cosmos with the new syncLocation
                        var writeItem = new ReplicaSet(readItem.id, true, readItem.databaseName, readItem.tableName, newLocation);

                        await container.UpsertItemAsync<ReplicaSet>(writeItem, partitionKey: new PartitionKey("11f76159-4b83-43dd-9eb9-282fd906219d"));
                    }
                    else
                    {
                        String sql = $"SELECT CHANGE_TRACKING_CURRENT_VERSION()";
                        long newLocation = 0;
                        using (SqlCommand command = new SqlCommand(sql, connectionForSource))
                        {
                            newLocation = (long)command.ExecuteScalar();
                        }

                        sql  =  "SELECT CityID, CityName, StateProvinceID, LatestRecordedPopulation, LastEditedBy " +
                                "FROM Application.Cities";

                        using (SqlCommand command = new SqlCommand(sql, connectionForSource))
                        {
                            SqlDataReader reader = command.ExecuteReader();

                            if (reader.HasRows)
                            {
                                using (SqlConnection connectionForDest = new SqlConnection(builderForDest.ConnectionString))
                                {
                                    int count=0;
                                    StringBuilder sb = new StringBuilder();
                                    connectionForDest.Open();
                                    while (reader.Read())
                                    {
                                        if (count == 0)
                                        {
                                            sb.Append($"INSERT INTO Application.Cities (CityID, CityName, StateProvinceID, LatestRecordedPopulation, LastEditedBy) VALUES (");
                                        }
                                        else
                                        {
                                            sb.Append(", (");
                                        }

                                        if (!reader.IsDBNull(reader.GetOrdinal("CityID")))
                                        {
                                             sb.Append($"{reader.GetInt32(reader.GetOrdinal("CityID"))}, ");
                                        }                      
                                        else
                                        {
                                            sb.Append("NULL, ");
                                        }              
                                        if (!reader.IsDBNull(reader.GetOrdinal("CityName")))
                                        {
                                            sb.Append($"'{reader.GetString(reader.GetOrdinal("CityName")).Replace("'", "''")}', ");
                                        }
                                        else
                                        {
                                            sb.Append("NULL, ");
                                        }   
                                        if (!reader.IsDBNull(reader.GetOrdinal("StateProvinceID")))
                                        {
                                            sb.Append($"{reader.GetInt32(reader.GetOrdinal("StateProvinceID"))}, ");
                                        }
                                        else
                                        {
                                            sb.Append("NULL, ");
                                        }   
                                        if (!reader.IsDBNull(reader.GetOrdinal("LatestRecordedPopulation")))
                                        {
                                            sb.Append($"{reader.GetInt64(reader.GetOrdinal("LatestRecordedPopulation"))}, ");
                                        }
                                        else
                                        {
                                            sb.Append("NULL, ");
                                        }   
                                        if (!reader.IsDBNull(reader.GetOrdinal("LastEditedBy")))
                                        {
                                            sb.Append($"{reader.GetInt32(reader.GetOrdinal("LastEditedBy"))} ");
                                        }
                                        else
                                        {
                                            sb.Append("NULL, ");
                                        }   
                                        sb.Append($")");
                                        
                                        if (count == 500)
                                        {
                                            using (SqlCommand commandAction = new SqlCommand(sb.ToString(), connectionForDest))
                                            {
                                                commandAction.ExecuteNonQuery();
                                            }
                                            sb.Clear();
                                            count = 0;
                                        }
                                        else
                                        {
                                            count++;
                                        }



                                    }
                                    
                                    using (SqlCommand commandAction = new SqlCommand(sb.ToString(), connectionForDest))
                                    {
                                        commandAction.ExecuteNonQuery();
                                    }
                                    
                                }
                                
                            }
                        }
                        //update cosmos with the new syncLocation
                        var writeItem = new ReplicaSet(readItem.id, true, readItem.databaseName, readItem.tableName, newLocation);

                        await container.UpsertItemAsync<ReplicaSet>(writeItem, partitionKey: new PartitionKey("11f76159-4b83-43dd-9eb9-282fd906219d"));

                    }


                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }
    }
}
