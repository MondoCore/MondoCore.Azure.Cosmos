using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

using MondoCore.Common;
using MondoCore.Data;

namespace MondoCore.Azure.Cosmos
{
    internal abstract class CosmosContainer<TID> 
    {
        private readonly IIdentifierStrategy<TID> _idStrategy;

        internal CosmosContainer(Container container, IIdentifierStrategy<TID> strategy)
        {
            this.Container = container;
            _idStrategy = strategy;
        }

        internal protected Container Container { get; }

        internal protected (string Id, PartitionKey PartitionKey) SplitId(TID id)
        {
            var sid = "";
            var partitionKey = PartitionKey.Null;

            if(id is IPartitionedId partitionedId)
            {
                sid = partitionedId.Id;
    
                if(!string.IsNullOrWhiteSpace(partitionedId.PartitionKey))
                { 
                    partitionKey = new PartitionKey(partitionedId.PartitionKey);

                    return (sid, partitionKey);
                }
            }

            if(_idStrategy != null)
            { 
                var idResult = _idStrategy.GetId(id);
                
                if(string.IsNullOrWhiteSpace(sid))
                    sid = idResult.Id;

                if(partitionKey == PartitionKey.Null)
                    partitionKey = new PartitionKey(idResult.PartitionKey);
            }

            return (id.ToString(), partitionKey);
        }
     
        protected async Task<TValue> InternalGet<TValue>(string id, PartitionKey partitionKey)
        {
            try
            { 
                var result = await this.Container.ReadItemAsync<TValue>(id, partitionKey);
            
                if(result == null)
                    throw new NotFoundException();

                return result;
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        protected IAsyncEnumerable<TValue> InternalGet<TValue>(Expression<Func<TValue, bool>> query)
        {
            return this.Container.GetItemLinqQueryable<TValue>()
                                 .Where(query)
                                 .ToFeedIterator()
                                 .ToAsyncEnumerable<TValue>();
        }
        
        protected string GetId<TValue>(TValue item)
        {
            if(item is IIdentifiable<TID> identifiable)
                return identifiable.Id.ToString();

            var id = item.GetValue<TID>("Id");

            return id.ToString();
        }
    }

    internal static class Extensions
    {
        internal static async IAsyncEnumerable<TModel> ToAsyncEnumerable<TModel>(this FeedIterator<TModel> setIterator)
        {
            while (setIterator.HasMoreResults)
            { 
                foreach (var item in await setIterator.ReadNextAsync())
                {
                    yield return item;
                }
            }

            setIterator.Dispose();
        }
    }
}
