using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using CsvHelper;
using System.Globalization;
using System.Net.Http;
using Newtonsoft.Json;

namespace csv_to_cosmos
{
    class Program
    {
        static void Main(string[] args)
        {
            //get csv data
            string csvFilename = ConfigurationManager.AppSettings["CsvFilename"];

            var items = new List<Dictionary<string, string>>();
            using (var stream = new FileStream(csvFilename, FileMode.Open))
            using (var reader = new StreamReader(stream))
            using (var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csvReader.Read();
                csvReader.ReadHeader();
                string[] headerRow = csvReader.Context.HeaderRecord;

                while (csvReader.Read())
                {
                    var dict = new Dictionary<string, string>();
                    foreach (string column in headerRow)
                    {
                        if (string.IsNullOrEmpty(column))
                            continue;
                        string value = csvReader.GetField(column);
                        if (string.IsNullOrEmpty(value))
                            continue;
                        dict.Add(column.ToLower().Trim(), value.ToLower().Trim());
                    }
                    items.Add(dict);
                }
            }

            Task.Run(async () =>
            {
                foreach (var row in items)
                {
                    if (row.ContainsKey("returnattribute"))
                    {
                        row.Add("key", string.Concat("-", row["module"], "-", row["returnattribute"]));
                    }

                    await PostBasicAsync(row);
                }
            }).Wait();

            Console.WriteLine("Finished");
            Console.ReadLine();
        }

        private static async Task PostBasicAsync(Dictionary<string,string> content)
        {
            string uri;
            if(content.ContainsKey("returnattribute"))
            {
                uri = @"https://powersecure-estimator-services-dev.azurewebsites.net/api/factors/";
            }
            else
            {
                uri = @"https://powersecure-estimator-services-dev.azurewebsites.net/api/functions/";
            }

            using (var client = new HttpClient())
            using (var request = new HttpRequestMessage(HttpMethod.Put, uri))
            {
                var json = JsonConvert.SerializeObject(content);
                using (var stringContent = new StringContent(json, Encoding.UTF8, "application/json"))
                {
                    request.Content = stringContent;

                    using (var response = await client
                        .SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
                        .ConfigureAwait(false))
                    {
                        response.EnsureSuccessStatusCode();
                    }
                }
            }
        }
    }
}
