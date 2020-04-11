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

namespace PowerAppsGuy.Covid19
{
    public static class CSSEGISandData_COVID_19_Daily_Report
    {
        [FunctionName("CSSEGISandData_COVID_19_Daily_Report")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            
            string filename = req.Query["filename"];

            int i = 0;
            int totalRecords = 0;

            JObject dailyReportObject = new JObject();

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            filename = filename ?? data?.filename;
            if(filename == null)
                filename = DateTime.Now.ToString("MM-dd-yyyy");
                
            HttpClient client = new HttpClient();
            try	
            {
                for(int d = 0; d < 1; d++) {
                    DateTime filedate = DateTime.ParseExact(req.Query["filename"], "MM-dd-yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    filedate = filedate.AddDays(d);
                    filename = filedate.ToString("MM-dd-yyyy") + ".csv";
                    HttpResponseMessage response = await client.GetAsync("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/" + filename);
                    response.EnsureSuccessStatusCode();
                    string responseBody = await response.Content.ReadAsStringAsync();
                    responseBody = responseBody.Replace("Country/Region", "Country_Region").Replace("Province/State", "Province_State").Replace("Last Update", "Last_Update");
                    
                    string accessToken = await GetAccessToken();

                    JObject countryRegionObject = new JObject();
                    JObject provinceStateObject = new JObject();
                    
                    string countryRegion = "";
                    string provinceState = "";

                    List<dailyReport> list = new List<dailyReport>();

                    using (var csv = new CsvReader(new StringReader(responseBody),System.Globalization.CultureInfo.InvariantCulture))
                    {
                        var records = csv.GetRecords<dailyReport>();
                        
                        foreach(dailyReport record in records) {
                            list.Add(record);
                        }
                        totalRecords = list.Count;

                        list.Sort();

                        foreach(dailyReport record in list) {
                            i++;
                            if(countryRegion != record.Country_Region) {
                                countryRegion = record.Country_Region;
                                HttpRequestMessage countryRegionRequest = createMessage(HttpMethod.Get, "pag_countryregions(pag_name='" + record.Country_Region.Replace("'","") + "')", accessToken);
                                var countryRegionResponse = await client.SendAsync(countryRegionRequest);
                                    
                                if(!countryRegionResponse.IsSuccessStatusCode) {
                                    HttpRequestMessage createCountryRegion = createMessage(HttpMethod.Post, "pag_countryregions", accessToken);
                                    countryRegionObject = new JObject();
                                    countryRegionObject.Add("pag_name", record.Country_Region.Replace("'",""));
                                    createCountryRegion.Content = new StringContent(countryRegionObject.ToString(), System.Text.Encoding.UTF8, "application/json");
                                    countryRegionResponse = await client.SendAsync(createCountryRegion);
                                    countryRegionResponse.EnsureSuccessStatusCode();
                                }
                                countryRegionObject = JObject.Parse(await countryRegionResponse.Content.ReadAsStringAsync());
                            }
                            if(provinceState != record.Province_State) {
                                provinceState = record.Province_State;
                                if(record.Province_State != "") {
                                    HttpRequestMessage provinceStateRequest = createMessage(HttpMethod.Get, "pag_provincestates?$filter=(pag_name eq '" + record.Province_State.Replace("'","") + "' and _pag_countryregion_value eq '" + countryRegionObject["pag_countryregionid"] + "')", accessToken);
                                    var provinceStateResponse = await client.SendAsync(provinceStateRequest);

                                    provinceStateObject = JObject.Parse(await provinceStateResponse.Content.ReadAsStringAsync());

                                    JArray items = (JArray)provinceStateObject["value"];
                                    int length = items.Count;

                                    if(items.Count == 0) {                                
                                        HttpRequestMessage createProvinceState = createMessage(HttpMethod.Post, "pag_provincestates", accessToken);
                                        provinceStateObject = new JObject();
                                        provinceStateObject.Add("pag_name", record.Province_State.Replace("'",""));
                                        provinceStateObject.Add("pag_CountryRegion@odata.bind", "/pag_countryregions(" + countryRegionObject["pag_countryregionid"] + ")");
                                        createProvinceState.Content = new StringContent(provinceStateObject.ToString(), System.Text.Encoding.UTF8, "application/json");
                                        provinceStateResponse = await client.SendAsync(createProvinceState);
                                        provinceStateResponse.EnsureSuccessStatusCode();
                                        provinceStateObject = JObject.Parse(await provinceStateResponse.Content.ReadAsStringAsync());
                                    }
                                    else
                                        provinceStateObject = (JObject)items[0];

                                }
                            }


                            HttpRequestMessage createDailyReport = createMessage(HttpMethod.Post, "pag_dailyreports", accessToken);

                            dailyReportObject = new JObject();
                            dailyReportObject.Add("pag_name", record.Country_Region.Replace("'","") + ", " + record.Province_State.Replace("'","") + " " + record.Last_Update);
                            Console.WriteLine(i.ToString() + " " + dailyReportObject["pag_name"]);
                            dailyReportObject.Add("pag_CountryRegion@odata.bind", "/pag_countryregions(" + countryRegionObject["pag_countryregionid"] + ")");
                            if(record.Province_State != "")
                                dailyReportObject.Add("pag_ProvinceState@odata.bind", "/pag_provincestates(" + provinceStateObject["pag_provincestateid"] + ")");
                            dailyReportObject.Add("pag_lastupdate", record.Last_Update);
                            dailyReportObject.Add("pag_deaths", record.Deaths);
                            dailyReportObject.Add("pag_confirmed", record.Confirmed);
                            dailyReportObject.Add("pag_recovered", record.Recovered);
                            dailyReportObject.Add("pag_filedate", filedate);
                            createDailyReport.Content = new StringContent(dailyReportObject.ToString(), System.Text.Encoding.UTF8, "application/json");
                            
                            var result = await client.SendAsync(createDailyReport);

                            var contents = await result.Content.ReadAsStringAsync();
                        }
                    }
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



            string responseMessage = $"Function executed successfully. Filename: {filename} - {i} records processed out of {totalRecords} & - {dailyReportObject}";

            return new OkObjectResult(responseMessage);
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
            HttpRequestMessage message =  new HttpRequestMessage(httpMethod, "https://org0b63707e.api.crm4.dynamics.com/api/data/v9.1/" + operation);

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

    class cr6f6_dailyReport
    {
        public string cr6f6_province_state { get; set; }
        public string cr6f6_country_region { get; set; }
        public DateTime cr6f6_last_update {get;set;}
        public float cr6f6_lat {get; set;}
        public float cr6f6_long {get;set;}
        public int cr6f6_confirmed {get;set;}
        public int cr6f6_deaths {get;set;}
        public int cr6f6_recovered {get;set;}
        
    }
}
