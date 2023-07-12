using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Newtonsoft.Json;
using Azure.Identity;
using Microsoft.Graph;
using Newtonsoft.Json.Linq;
using ClosedXML.Excel;
using Microsoft.Extensions.Configuration;
using Remitee.Services.Metrics.Extensions;

namespace Remitee.Services.Metrics.Controllers
{
    public class SpreadSheetConnector
    {
        private readonly string clientId;
        private readonly string clientSecret;
        private readonly string tenantId;
        private readonly string secretId;
        private readonly string objectId;
        private readonly string driveId;
        private readonly string itemId;
        private readonly string sharedFile;
        public string fileLocation;

        public SpreadSheetConnector(IConfigurationRoot config)
        {
            clientId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("ClientId").Value;
            clientSecret = config.GetSection("AppSettings").GetSection("Credentials").GetSection("ClientSecret").Value;
            tenantId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("TenantId").Value;
            secretId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("SecretId").Value;
            objectId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("ObjectId").Value;
            driveId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("DriveId").Value;
            itemId = config.GetSection("AppSettings").GetSection("Credentials").GetSection("ItemId").Value;
            sharedFile = config.GetSection("AppSettings").GetSection("Credentials").GetSection("SharedFile").Value;
        }

        public async Task DownloadSharedFile()
        {
            var credentials = new ClientSecretCredential(
                tenantId,
                clientId,
                clientSecret,
                new TokenCredentialOptions { AuthorityHost = AzureAuthorityHosts.AzurePublicCloud });

            GraphServiceClient graphClient = new GraphServiceClient(credentials);

            string base64Value = System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(sharedFile));
            string encodedUrl = "u!" + base64Value.TrimEnd('=').Replace('/', '_').Replace('+', '-');

            var item = await graphClient.Shares[encodedUrl].DriveItem.GetAsync();

            var request = graphClient
                .Drives[driveId]
                .Items[itemId]
                .Content
                .ToGetRequestInformation();

            //request.UrlTemplate = request.UrlTemplate.Insert(request.UrlTemplate.Length, "/$value");
            var attachmentStream = graphClient.RequestAdapter.SendPrimitiveAsync<System.IO.Stream>(request).GetAwaiter().GetResult();
            using (FileStream fs = new FileStream(@"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Remitee.Services.Metrics\Descargas\" + item.Name, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize: 8 * 1024, useAsync: true))
            {
                attachmentStream.Seek(0, SeekOrigin.Begin);
                attachmentStream.CopyTo(fs);
            }
            fileLocation = @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Remitee.Services.Metrics\Descargas\" + item.Name;
        }


        // Pass in your data as a list of a list (2-D lists are equivalent to the 2-D spreadsheet structure)
        public async Task<string> UpdateData(List<IList<object>> data,String range)
        {
            
            return "";

        }

        public string AppendData(List<IList<object>> data, String sheet)
        {


            

            return "";
        }

        public List<IList<object>> ReadData(String range, string file)
        {
            var result = new List<IList<object>>();
            var wb = new XLWorkbook(file);
            IXLWorksheet ws;
            var values = wb.RangeFromFullAddress(range, out ws);
            foreach(var row in values.Rows())
            {
                result.Add(row.Cells().Select(x => x.ParseCell()).ToList());
            }

            return result;


        }

        private string GetExcelColumnName(int columnNumber)
        {
            string columnName = "";

            while (columnNumber > 0)
            {
                int modulo = (columnNumber - 1) % 26;
                columnName = Convert.ToChar('A' + modulo) + columnName;
                columnNumber = (columnNumber - modulo) / 26;
            }

            return columnName;
        }

    }


}
