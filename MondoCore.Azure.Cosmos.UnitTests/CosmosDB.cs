
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Microsoft.Azure.Cosmos;
using MondoCore.Data;
using MondoCore.Repository.TestHelper;

namespace MondoCore.Azure.Cosmos.UnitTests
{
    [TestClass]
    public sealed class CosmosDBTests : RepositoryTestBase
    {
        // This is for emulator
        private const string _accountEndpoint        = "https://localhost:8081/";
        private const string _authKeyOrResourceToken = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

       
        public CosmosDBTests() :

           base(new CosmosDB("testdb", _accountEndpoint, _authKeyOrResourceToken),
                "testcontainer",
                ()=> Guid.NewGuid().ToString())
        {
        }
    }
}
