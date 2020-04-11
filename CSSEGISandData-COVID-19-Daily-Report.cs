using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using CsvHelper;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Collections.Generic;
using System.Linq;

namespace PowerAppsGuy.Covid19
{
    public static class CSSEGISandData_COVID_19_Daily_Report
    {


        [FunctionName("CSSEGISandData_COVID_19_Daily_Report")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            
            string accessToken = await GetAccessToken();

            string apiUrl = Environment.GetEnvironmentVariable("apiUrl");

            string filename = req.Query["filename"];

            int i = 0;
            int totalRecords = 0;

            JObject dailyReportObject = new JObject();
            List<JObject> dailyReportBatch = new List<JObject>();

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            filename = filename ?? data?.filename;
            if(filename == null)
                filename = DateTime.Now.ToString("MM-dd-yyyy");
                
            HttpClient client = new HttpClient();

            try	{
                // get file date
                DateTime filedate = DateTime.ParseExact(req.Query["filename"], "MM-dd-yyyy", System.Globalization.CultureInfo.InvariantCulture);
                filename = filedate.ToString("MM-dd-yyyy") + ".csv";

                // get daily report from John Hopkins CSSE repository
                HttpResponseMessage response = await client.GetAsync("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/" + filename);
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();
                responseBody = responseBody.Replace("Country/Region", "Country_Region").Replace("Province/State", "Province_State").Replace("Last Update", "Last_Update");
                
                JObject countryRegionObject = new JObject();
                JObject provinceStateObject = new JObject();

                using (var csv = new CsvReader(new StringReader(responseBody),System.Globalization.CultureInfo.InvariantCulture))
                {
                    List<dailyReport> list = new List<dailyReport>();                        
                    foreach(dailyReport record in csv.GetRecords<dailyReport>()) {
                        list.Add(record);
                    }
                    
                    list.Sort();

                    IEnumerable<dailyReport> l = list;

                    var grouped = l.GroupBy(x => new { x.Country_Region, x.Province_State }).Select(g => new
                        {
                            Name = g.Key,
                            Deaths = g.Sum(x => (int)x.Deaths),
                            Confirmed = g.Sum(x => (int)x.Confirmed),
                            Recovered = g.Sum(x => (int)x.Recovered)
                        });;

                    totalRecords = grouped.Count();

                    foreach(var rec in grouped) {
                        // if country / region does not exist then create it
                        countryRegionObject = await countryRegionCreateIfNotExist(rec.Name.Country_Region, client, accessToken);

                        if(rec.Name.Province_State != "") {
                            // if province / state does not exist then create it
                            provinceStateObject = await provinceStateCreateIfNotExist(countryRegionObject, rec.Name.Province_State, client, accessToken);
                        }

                        dailyReportObject = new JObject();
                        string name = countryRegionObject["pag_name"].ToString();
                        if(rec.Name.Province_State != "")
                            name = name + ", " + provinceStateObject["pag_name"].ToString();
                    
                        dailyReportObject.Add("pag_name",  name);
                        Console.WriteLine(i.ToString() + " " + dailyReportObject["pag_name"]);
                        dailyReportObject.Add("pag_CountryRegion@odata.bind", "/pag_countryregions(" + countryRegionObject["pag_countryregionid"] + ")");
                        if(rec.Name.Province_State != "")
                            dailyReportObject.Add("pag_ProvinceState@odata.bind", "/pag_provincestates(" + provinceStateObject["pag_provincestateid"] + ")");
                        dailyReportObject.Add("pag_deaths", rec.Deaths);
                        dailyReportObject.Add("pag_confirmed", rec.Confirmed);
                        dailyReportObject.Add("pag_recovered", rec.Recovered);
                        dailyReportObject.Add("pag_filedate", filedate);

                        dailyReportBatch.Add(dailyReportObject);
                    }

                    //Init Batch
                    string batchName = $"batch_{Guid.NewGuid()}";
                    MultipartContent batchContent = new MultipartContent("mixed", batchName);

                    string changesetName = $"changeset_{Guid.NewGuid()}";
                    MultipartContent changesetContent = new MultipartContent("mixed", changesetName);
                    for(int j=0; j<dailyReportBatch.Count;j++)
                    {
                        HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, apiUrl + "pag_dailyreports");
                        requestMessage.Version = new Version(1,1);
                        HttpMessageContent messageContent = new HttpMessageContent(requestMessage);
                        messageContent.Headers.Remove("Content-Type");
                        messageContent.Headers.Add("Content-Type", "application/http");
                        messageContent.Headers.Add("Content-Transfer-Encoding", "binary");

                        StringContent stringContent = new StringContent(dailyReportBatch[j].ToString());
                        stringContent.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json;type=entry");
                        requestMessage.Content = stringContent;
                        messageContent.Headers.Add("Content-ID", j.ToString());
                        
                        changesetContent.Add(messageContent);
                    }
                    batchContent.Add(changesetContent);

                    HttpRequestMessage batchRequest = new HttpRequestMessage(HttpMethod.Post, apiUrl + "$batch");
                    batchRequest.Version = new Version(1,1);
                    batchRequest.Content = batchContent;
                    batchRequest.Headers.Add("Prefer", "odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\"");
                    batchRequest.Headers.Add("OData-MaxVersion", "4.0");
                    batchRequest.Headers.Add("OData-Version", "4.0");
                    batchRequest.Headers.Add("Accept", "application/json");
                    batchRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

                    //Execute Batch request
                    HttpResponseMessage batchResponse = await client.SendAsync(batchRequest);

                    //clear list of records
                    dailyReportBatch.Clear();
                }
            }  
            catch(CsvHelper.CsvHelperException ex)
            {
                Console.WriteLine(ex.Data["CsvHelper"]);
            }
            catch(HttpRequestException e)
            {
                Console.WriteLine("\nException Caught!");	
                Console.WriteLine("Message :{0} ",e.Message);
            }

            string responseMessage = $"Function executed successfully. Filename: {filename} processed. {totalRecords} records written";

            return new OkObjectResult(responseMessage);
        }

        private static async Task<JObject> countryRegionCreateIfNotExist(string countryRegion, HttpClient client, string accessToken) {
            // check if country / region exist
            HttpRequestMessage countryRegionRequest = createMessage(HttpMethod.Get, "pag_countryregions(pag_name='" + countryRegion.Replace("'","") + "')", accessToken);
            var countryRegionResponse = await client.SendAsync(countryRegionRequest);

            // if country does not exist create country / region
            if(!countryRegionResponse.IsSuccessStatusCode) {
                HttpRequestMessage createCountryRegion = createMessage(HttpMethod.Post, "pag_countryregions", accessToken);
                JObject countryRegionObject = new JObject();
                countryRegionObject.Add("pag_name", countryRegion.Replace("'",""));
                createCountryRegion.Content = new StringContent(countryRegionObject.ToString(), System.Text.Encoding.UTF8, "application/json");
                countryRegionResponse = await client.SendAsync(createCountryRegion);
                countryRegionResponse.EnsureSuccessStatusCode();
            }

            return JObject.Parse(await countryRegionResponse.Content.ReadAsStringAsync());
        }

        private static async Task<JObject> provinceStateCreateIfNotExist(JObject countryRegionObject, string provinceState, HttpClient client, string accessToken) {
            // check if province / state exist
            HttpRequestMessage provinceStateRequest = createMessage(HttpMethod.Get, "pag_provincestates?$filter=(pag_name eq '" + provinceState.Replace("'","") + "' and _pag_countryregion_value eq '" + countryRegionObject["pag_countryregionid"] + "')", accessToken);
            var provinceStateResponse = await client.SendAsync(provinceStateRequest);

            JObject provinceStateObject = JObject.Parse(await provinceStateResponse.Content.ReadAsStringAsync());

            JArray items = (JArray)provinceStateObject["value"];
            
            // if province / state does not exist create province / state
            if(items.Count == 0) {                                
                HttpRequestMessage createProvinceState = createMessage(HttpMethod.Post, "pag_provincestates", accessToken);
                provinceStateObject = new JObject();
                provinceStateObject.Add("pag_name", provinceState.Replace("'",""));
                provinceStateObject.Add("pag_CountryRegion@odata.bind", "/pag_countryregions(" + countryRegionObject["pag_countryregionid"] + ")");
                createProvinceState.Content = new StringContent(provinceStateObject.ToString(), System.Text.Encoding.UTF8, "application/json");
                provinceStateResponse = await client.SendAsync(createProvinceState);
                provinceStateResponse.EnsureSuccessStatusCode();
                provinceStateObject = JObject.Parse(await provinceStateResponse.Content.ReadAsStringAsync());
            }
            else
                provinceStateObject = (JObject)items[0];

            return provinceStateObject;
        }


        private static async Task<string> GetAccessToken()
        {
            String clientId = Environment.GetEnvironmentVariable("clientId");
            String secret = Environment.GetEnvironmentVariable("secret");
            String tenantId = Environment.GetEnvironmentVariable("tenantId");
            String resourceUrl = Environment.GetEnvironmentVariable("resourceUrl");

            var credentials = new ClientCredential(clientId, secret);
            var authContext = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);
            var result = await authContext.AcquireTokenAsync(resourceUrl, credentials);

            return result.AccessToken;
        }

        private static HttpRequestMessage createMessage(HttpMethod httpMethod, string operation, string accessToken) {
            HttpRequestMessage message =  new HttpRequestMessage(httpMethod, Environment.GetEnvironmentVariable("apiUrl") + operation);

            message.Headers.Add("OData-MaxVersion", "4.0");
            message.Headers.Add("OData-Version", "4.0");
            message.Headers.Add("Prefer", "return=representation");
            message.Headers.Add("Prefer", "odata.include-annotations=*");
            message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            return message;
        }
    }

    class dailyReport : IEquatable<dailyReport>, IComparable<dailyReport>
    {
        public string Province_State { get; set; }
        public string Country_Region { get; set; }
        public string Last_Update {get;set;}
        public int? Confirmed {get;set;}
        public int? Deaths {get;set;}
        public int? Recovered {get;set;}
        
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            dailyReport dailyReportObj = obj as dailyReport;
            if (dailyReportObj == null) return false;
            else return Equals(dailyReportObj);
        }
        public int SortByNameAscending(string name1, string name2)
        {
            
            return name1.CompareTo(name2);
        }

        // Default comparer for dailyReport type.
        public int CompareTo(dailyReport compareDailyReport)
        {
            // A null value means that this object is greater.
            if (compareDailyReport == null)
                return 1;
                
            else {

            
                int result = this.Country_Region.CompareTo(compareDailyReport.Country_Region);
                if(result == 0) {
                    result = this.Province_State.CompareTo(compareDailyReport.Province_State);
                }
                return result;
            }
        }
        public override int GetHashCode()
        {
            return Country_Region.GetHashCode();
        }
        public bool Equals(dailyReport other)
        {
            if (other == null) return false;
            return (this.Country_Region.Equals(other.Country_Region));
        }
    }
}
