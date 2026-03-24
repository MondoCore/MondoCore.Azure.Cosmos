using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using MondoCore.Common;
using MondoCore.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace MondoCore.Azure.Cosmos
{
   internal class CosmosContainerWriter<TID, TValue> : CosmosContainer<TID>, IWriteRepository<TID, TValue> where TValue : IIdentifiable<TID> 
    {
        internal CosmosContainerWriter(Container container, IIdentifierStrategy<TID> strategy) : base(container, strategy)
        {
        }

       #region IWriteRepository

        public async Task<bool> Delete(TID id, CancellationToken cancellationToken = default)
        {
            var idResult = SplitId(id);
                
            await this.Container.DeleteItemAsync<TValue>(idResult.Id, idResult.PartitionKey, cancellationToken: cancellationToken).ConfigureAwait(false);

            return true;
        }

        public async Task<long> Delete(Expression<Func<TValue, bool>> guard, CancellationToken cancellationToken = default)
        {
            var result = InternalGet<TValue>(guard, cancellationToken);
            var count = 0L;

            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                var partitionKey = GetPartitionKey(val);

                try
                { 
                    await this.Container.DeleteItemAsync<TValue>(GetId(val), partitionKey, cancellationToken: token).ConfigureAwait(false);

                    Interlocked.Increment(ref count);
                }
                catch
                {
                }
            }).ConfigureAwait(false);

            return count;
        }

        public async Task<TValue> Insert(TValue item, CancellationToken cancellationToken = default)
        {
            var result = await this.Container.CreateItemAsync(item, cancellationToken: cancellationToken).ConfigureAwait(false);

            return result.Resource;
        }

        public async Task Insert(IEnumerable<TValue> items, CancellationToken cancellationToken = default)
        {
            await Parallel.ForEachAsync(items, async (val, token)=>
            {
                await Insert(val, token).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }

        public async Task<bool> Update(TValue item, Expression<Func<TValue, bool>> guard = null, CancellationToken cancellationToken = default)
        {
            if(guard != null)
            { 
                var currentItem = await InternalGet<TValue>(GetId(item), GetPartitionKey(item), cancellationToken: cancellationToken).ConfigureAwait(false);
                var list        = (new List<TValue> {currentItem}) as IEnumerable<TValue>;
                var fnGuard     = guard.Compile();

                if(!list.Where(fnGuard).Any())
                    return false;
            }

            var partitionKey = GetPartitionKey(item);

            try
            { 
                var result = await this.Container.UpsertItemAsync(item, partitionKey, cancellationToken: cancellationToken).ConfigureAwait(false);

                return result.StatusCode == System.Net.HttpStatusCode.OK;
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        public async Task<long> Update(object properties, Expression<Func<TValue, bool>> query, CancellationToken cancellationToken = default)
        {
            var result = InternalGet<TValue>(query, cancellationToken: cancellationToken); 
            var count = 0L;

            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                try
                { 
                    if(val.SetValues(properties))
                    { 
                        await this.Container.UpsertItemAsync<TValue>(val, cancellationToken: token);
                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            }).ConfigureAwait(false);

            return count;
        }

        public async Task<long> Update(Func<TValue, Task<(bool Update, bool Continue)>> update, Expression<Func<TValue, bool>> query, CancellationToken cancellationToken = default)
        {
            var result = InternalGet<TValue>(query, cancellationToken: cancellationToken); 
            var count = 0L;
            
            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                try
                { 
                    var each = await update(val);

                    if(each.Update)
                    { 
                        await this.Container.UpsertItemAsync<TValue>(val, cancellationToken: cancellationToken).ConfigureAwait(false);
                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            }).ConfigureAwait(false);

            return count;
        }

        #endregion

        private PartitionKey GetPartitionKey(TValue item)
        {
            if(item is IPartitionable<TID> partitionable)
                return new PartitionKey(partitionable.GetPartitionKey());

            return PartitionKey.None;
        }
    }
}
