using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

using System.Linq;

using MondoCore.Data;

namespace MondoCore.Repository.TestHelper
{
    public class RepositoryTestBase
    {
        protected IDatabase                         _db;
        protected IReadRepository<string, Automobile>  _reader;
        protected IWriteRepository<string, Automobile> _writer;
        protected readonly Func<string>                _createNewId;

        protected readonly string _repoName;

        protected List<string> _idCollection = new List<string>();

        protected RepositoryTestBase(IDatabase db, string repoName, Func<string> createNewId)
        {
            _db          = db;
            _writer      = _db.GetRepositoryWriter<string, Automobile>(repoName, "Chevy");
            _reader      = _db.GetRepositoryReader<string, Automobile>(repoName, "Chevy");
            _createNewId = createNewId;
            _repoName    = repoName;
        }

       
        private async Task Initialize()
        {
            await _writer.Delete( _=> true );

            _idCollection.Clear();

            for(var i = 0; i < 6; ++i)
                _idCollection.Add(_createNewId());

            await _writer.Insert(new Automobile { Id = _idCollection[0], Make = "Chevy",      Color = "Blue",  Model = "Camaro",    Year = 1969 });
            await _writer.Insert(new Automobile { Id = _idCollection[1], Make = "Pontiac",    Color = "Black", Model = "Firebird",  Year = 1972 });
            await _writer.Insert(new Automobile { Id = _idCollection[2], Make = "Chevy",      Color = "Green", Model = "Corvette",  Year = 1964 });
            await _writer.Insert(new Automobile { Id = _idCollection[3], Make = "Audi",       Color = "Blue",  Model = "S5",        Year = 2021 });
            await _writer.Insert(new Automobile { Id = _idCollection[4], Make = "Studebaker", Color = "Black", Model = "Speedster", Year = 1914 });
            await _writer.Insert(new Automobile { Id = _idCollection[5], Make = "Arrow",      Color = "Green", Model = "Glow",      Year = 1917 });
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            await _writer.Delete( _=> true );

            _idCollection.Clear();
        }

        [TestMethod]
        public async Task Writer_Insert()
        {
            await Initialize();

            var id = _createNewId();

            await _writer.Insert(new Automobile 
            {
               Id = id,
               Make = "Chevy",
               Model = "GTO",
               Color = "Dark Blue",
               Year = 1972
            });

            var result = await _reader.Get(id);

            Assert.IsNotNull(result);
            Assert.AreEqual(id, result.Id);
            Assert.AreEqual("GTO", result.Model);

            await _writer.Delete(id);
        }


        [TestMethod]
        public async Task Writer_Delete()
        {
            await Initialize();

            await _writer.Delete( _=> true );

            var result = _reader.Get(_=> true);
            var list = await result.ToListAsync();

            Assert.IsNotNull(result);
            Assert.AreEqual(0, list.Count);
        }

        [TestMethod]
        public async Task Writer_Delete_query()
        {
            await Initialize();

            await _writer.Delete( (i)=> i.Make == "Chevy" );

            var result = _reader.Get(_=> true);
            var list = await result.ToListAsync();

            Assert.IsNotNull(result);
            Assert.AreEqual(4, list.Count);
        }

        [TestMethod]
        public async Task Writer_Insert_many()
        {
            await Initialize();

            var id1 = _createNewId();
            var id2 = _createNewId();

            await _writer.Insert(new List<Automobile> 
            {
                new Automobile 
                {
                   Id = id1,
                   Make = "Pontiac",
                   Model = "GTO",
                   Color = "Dark Blue",
                   Year = 1972
                },
                new Automobile 
                {
                   Id = id2,
                   Make = "Aston-Martin",
                   Model = "DB9",
                   Color = "Cobalt",
                   Year = 1968
                }
            });

            _reader = _db.GetRepositoryReader<string, Automobile>(_repoName, "Pontiac");

            var result1 = await _reader.Get(id1);

            Assert.IsNotNull(result1);
            Assert.AreEqual(id1, result1.Id);
            Assert.AreEqual("GTO", result1.Model);

            _reader = _db.GetRepositoryReader<string, Automobile>(_repoName, "Aston-Martin");

            var result2 = await _reader.Get(id2);

            Assert.IsNotNull(result2);
            Assert.AreEqual(id2, result2.Id);
            Assert.AreEqual("DB9", result2.Model);
        }

        [TestMethod]
        public async Task Reader_Get_notfound()
        {
            await Initialize();

            var id = _createNewId();
            await _writer.Insert(new Automobile { Id = id, Make = "Chevy", Model = "Camaro" });

            await Assert.ThrowsExceptionAsync<NotFoundException>( async ()=> await _reader.Get(_createNewId()));
        }

        [TestMethod]
        public async Task Reader_Where()
        {
            await Initialize();

            var result = _reader.Where( o=> o.Make == "Chevy").ToList();

            Assert.AreEqual(2, result.Count());
            Assert.IsTrue(result.Where( c=> c.Model == "Corvette").Any());
            Assert.IsTrue(result.Where( c=> c.Model == "Camaro").Any());
        }

        [TestMethod]
        public async Task Reader_Where_Pontiac()
        {
            await Initialize();

            var result = _reader.Where( o=> o.Make == "Pontiac").ToList();

            Assert.AreEqual(1, result.Count());
            Assert.IsTrue(result.Where( c=> c.Model == "Firebird").Any());
        }

        [TestMethod]
        public async Task Reader_Where_Id()
        {
            await Initialize();

            var result = _reader.Where( o=> o.Id!.ToString() == _idCollection[1]!.ToString()).ToList();

            Assert.AreEqual(1, result.Count());
            Assert.IsTrue(result.Where( c=> c.Model == "Firebird").Any());
        }

        [TestMethod]
        public async Task Reader_Get_wExpression()
        {
            await Initialize();

            var result = await _reader.Get( o=> o.Make == "Chevy").ToListAsync();

            Assert.AreEqual(2, result.Count());
            Assert.IsTrue(result.Where( c=> c.Model == "Corvette").Any());
            Assert.IsTrue(result.Where( c=> c.Model == "Camaro").Any());

            var result2 = await _reader.Get( o=> o.Year < 1970).ToListAsync();

            Assert.AreEqual(4, result2.Count());
        }

        [TestMethod]
        public async Task Reader_Get_wExpression_all()
        {
            await Initialize();

            var result = _reader.Get( _=> true);
            var count = 0;

            await foreach ( var o in result)
                count++;

            Assert.AreEqual(6, count);

            result = _reader.Get( _=> true);
            var list = await result.ToListAsync();

            Assert.IsTrue(list.Where( c=> c.Model == "Corvette").Any());
            Assert.IsTrue(list.Where( c=> c.Model == "Camaro").Any());
        }

        [TestMethod]
        public async Task Reader_Get_wId_list()
        {
            await Initialize();

            var reader = _db.GetRepositoryReader<string, Automobile>(_repoName, "Chevy");
            var id1 = _idCollection[0];
            var id2 = _idCollection[2];

            var result = await reader.Get( new List<string> { id1, id2 } ).ToListAsync();

            Assert.AreEqual(2, result.Count());

            Assert.IsTrue(result.Where( c=> c.Model == "Corvette").Any());
            Assert.IsTrue(result.Where( c=> c.Model == "Camaro").Any());
        }

        [TestMethod]
        public async Task Reader_Get_wExpression_notfound()
        {
            await Initialize();

            var result = await _reader.Get( o=> o.Make == "Chevy").ToListAsync();

            Assert.AreEqual(2, result.Count());
            Assert.IsTrue(result.Where( c=> c.Model == "Corvette").Any());
            Assert.IsTrue(result.Where( c=> c.Model == "Camaro").Any());

            var result2 = await _reader.Get( o=> o.Year < 1900).ToListAsync();

            Assert.AreEqual(0, result2.Count);
        }

        [TestMethod]
        public async Task Writer_Update()
        {
            await Initialize();

            Assert.IsTrue(await _writer.Update(new Automobile { Id = _idCollection[0], Make = "Chevy", Model = "Camaro", Year = 1970 }));  

            var result = await _reader.Get(_idCollection[0]);

            Assert.AreEqual(1970, result.Year);
        }

        [TestMethod]
        public async Task Writer_Update_wGuard_succeeds()
        {
            await Initialize();

            var result = await _writer.Update(new Automobile { Id = _idCollection[0], Make = "Chevy", Model = "Camaro", Color = "Blue", Year = 1970 }, (i)=> i.Color == "Blue");  

            Assert.IsTrue(result);  

            var result1 = await _reader.Get(_idCollection[0]);

            Assert.AreEqual(1970, result1.Year);
        }

        [TestMethod]
        public async Task Writer_Update_wGuard_fails()
        {
            await Initialize();

            await _writer.Update(new Automobile { Id = _idCollection[0], Make = "Chevy", Model = "Camaro", Year = 1970 }, (i)=> i.Color == "Periwinkle");  

            var result1 = await _reader.Get(_idCollection[0]);

            Assert.AreEqual(1969, result1.Year);
        }
        
        [TestMethod]
        public async Task Writer_Update_properties_succeeds()
        {
            await Initialize();

            Assert.AreEqual(2, await _writer.Update(new { Year = 1970 }, (i)=> i.Color == "Blue"));  

            var result1 = await _reader.Get(_idCollection[0]);

            Assert.AreEqual(1970, result1.Year);
        }

        [TestMethod]
        public async Task Writer_Update_properties_2vals_succeeds()
        {
            await Initialize();

            Assert.AreEqual(2, await _writer.Update(new { Year = 1970, Color = "Red" }, (i)=> i.Color == "Blue"));  

            var result1 = await _reader.Get(_idCollection[0]);

            Assert.AreEqual(1970, result1.Year);

            Assert.AreEqual("Red",   result1.Color);
        }

        [TestMethod]
        public async Task Writer_Update_lambda_succeeds()
        {
            await Initialize();

            var numUpdated = await _writer.Update((i)=> 
            {
                i.Year = 1970;

                return Task.FromResult((true, true));
            },
            (i2)=> i2.Color != "Green");  

            Assert.AreEqual(4, numUpdated);  
            
            var reader = _db.GetRepositoryReader<string, Automobile>(_repoName, "Chevy");

            var result1 = (await reader.Get( (i)=> i.Id == _idCollection[0]).ToListAsync()).FirstOrDefault();
            var result2 = (await reader.Get( (i)=> i.Id == _idCollection[1]).ToListAsync()).FirstOrDefault();
            var result3 = (await reader.Get( (i)=> i.Id == _idCollection[2]).ToListAsync()).FirstOrDefault();
            var result4 = (await reader.Get( (i)=> i.Id == _idCollection[3]).ToListAsync()).FirstOrDefault();
            var result5 = (await reader.Get( (i)=> i.Id == _idCollection[4]).ToListAsync()).FirstOrDefault();
            var result6 = (await reader.Get( (i)=> i.Id == _idCollection[5]).ToListAsync()).FirstOrDefault();

            Assert.AreEqual(1970, result1!.Year);
            Assert.AreEqual(1970, result2!.Year);
            Assert.AreEqual(1964, result3!.Year);
            Assert.AreEqual(1970, result4!.Year);
            Assert.AreEqual(1970, result5!.Year);
            Assert.AreEqual(1917, result6!.Year);
        }

        public class Automobile : IPartitionable<string>
        {
            [JsonPropertyName("id")]
            public string?     Id    {get; set;}
            public string?     id    => Id;

            public string   Make  {get; set;} = "";
            public string   Model {get; set;} = "";
            public string   Color {get; set;} = "";
            public int      Year  {get; set;} = 1964;

            public string GetPartitionKey()
            {
                return this.Make;
            }

            public override string ToString()
            {
                return JsonSerializer.Serialize(this);
            }
        }
    }
}
