let async = require('async'),
    collect = require('collect.js'),
    crypto = require('crypto'),
    Queue = require('better-queue'),
    log4js = require('log4js'),
    once = require('once'),
    moment = require('moment');

class RedisPriorityQueue{
    constructor(RedisClient, QueueName, GroupKey, options) {
        this.RedisClient = RedisClient;
        this.QueueName = QueueName;

        this.options = options || {
            ActionTimeout: 10 * 1000,
        };

        this.GroupKey = GroupKey;

        this._isFetchingItems = false;
        this._logger = log4js.getLogger('RedisPriorityQueue');

        this.TaskQueue = new Queue(function (task, cb) {
            return task(cb);
        }, {
            maxRetries: 0,
            concurrent: 1,
        });

        setInterval(() => {
            this._logger.info('RedisPriorityInternalQueueStats', this.TaskQueue.getStats());
        }, 15000);

        setInterval(() => {
            this._logger.info('RedisPriorityInternalQueueStats Reset', this.TaskQueue.resetStats());
        }, 60 * 60 * 1000);
    }

    fetchingItems(action, interval = 0.3){
        this._isFetchingItems = true;

        let fetchingJob = async (cb) => {
            cb = once('cb')
            if(this._isFetchingItems){
                try{
                    let StartTime = Date.now(),
                        fetchedItem = await this.pullHighestItem();

                    if(fetchedItem){
                        action(fetchedItem['content'], (err, result) => {
                            this._logger.debug('action finished', err, result)
                            this.TaskQueue.push(async (cb) => {
                                fetchingJob(cb);
                            });
                            cb(null);
                        })
                    }else{
                        setTimeout(() => {
                            this.TaskQueue.push(async (cb) => {
                                fetchingJob(cb);
                            });
                        }, interval * 1000)
                        return cb(null);
                    }
                }catch (e) {
                    this._logger.error(e);

                    // If Error is occurred, retry fetching after waiting for 1 seconds
                    setTimeout(() => {
                        this.TaskQueue.push(async (cb) => {
                            fetchingJob(cb);
                        });
                    }, 1000)
                    return cb(null);
                }
            }else{
                setTimeout(() => {
                    this.TaskQueue.push(async (cb) => {
                        fetchingJob(cb);
                    })
                }, interval * 1000)
                return cb(null);
            }
        }

        this.TaskQueue.push(async (cb) => {
            fetchingJob(cb);
        });
    }

    getQueueItems(){
        return new Promise((resolve, reject) => {
            this.RedisClient.send_command('ZRANGEBYSCORE', [this.QueueName, 0, 100, 'WITHSCORES'], (err, result) => {
                if(result.length == 0){
                    return resolve(null)
                }else{
                    let f_result = [];
                    for(let i=0; i < result.length; i+=2){
                        f_result.push({
                            content: this._isJSON(result[i]) ? JSON.parse(result[i]) : result[i],
                            weight: parseFloat(result[i+1])
                        })
                    }
                    return resolve(f_result);
                }
            })
        });
    }

    addToQueue(item, weight){
        this._logger.info('addTo Queue', item, weight);
        return new Promise((resolve, reject) => {
            item['hash'] = crypto.createHash('sha1').update(item['hash_name'] + item['timestamp'] + Math.floor(Math.random() * 1256952152)).digest('hex').toUpperCase()
            this.RedisClient.send_command('ZADD', [this.QueueName, weight, JSON.stringify(item)], (err, result) => {
                if(err){
                    this._logger.error(err)
                    return reject(err)
                }
                this._logger.info('add result:', result)

                return resolve();
            })
        });
    }

    batchAddToQueue(Items){
        return new Promise((resolve, reject) => {
            if(!Items || Items.length <= 0){
                return resolve();
            }

            let command = [this.QueueName];
            Items.forEach((Item) => {
                let {content, weight} = Item;
                this._logger.info('addTo Queue', Item);
                content['hash'] = crypto.createHash('sha1').update(content['hash_name'] + content['timestamp'] + Math.floor(Math.random() * 1256952152)).digest('hex').toUpperCase()
                command.push(weight);
                command.push(JSON.stringify(content));

            });
            this.RedisClient.send_command('ZADD', command, (err, result) => {
                if(err){
                    this._logger.error(err)
                    return reject(err)
                }
                this._logger.info('add result:', result);

                return resolve();
            })
        });
    }

    getHighestItem(isRaw=false){
        return new Promise((resolve, reject) => {
            this.RedisClient.send_command('zrange', [this.QueueName, 0, 0, 'withscores'], (err, result) => {
                if(err)
                    return reject(err)

                if(result && result.length > 0){
                    resolve({
                        content: isRaw ? result[0] : (this._isJSON(result[0]) ? JSON.parse(result[0]) : result[0]),
                        weight: parseFloat(result[1])
                    });
                }else{
                    return resolve(null)
                }
            })
        });
    }

    async resetQueue(Items){
        await this.deleteQueue();
        return this.batchAddToQueue(Items);
    }

    pullHighestItem(){
        return new Promise(async (resolve, reject) => {
            let HighestItem = await this.getHighestItem(true);
            if(HighestItem){
                try{
                    this._logger.error('HighestItem:', this._isJSON(HighestItem) ? JSON.parse(HighestItem) : HighestItem);
                }catch (e) {
                    this._logger.error(e);
                }

                this.RedisClient.send_command('ZREM', [this.QueueName, HighestItem['content']], (err, del_result) => {
                    if(err)
                        return reject(err)

                    this._logger.info('Delete Highest Item:', del_result);
                    if(this._isJSON(HighestItem['content'])){
                        HighestItem['content'] = JSON.parse(HighestItem['content'])
                    }
                    return resolve(HighestItem)
                })
            }else{
                return resolve(null)
            }
        });
    }

    getAmount(){
        return new Promise((resolve, reject) => {
            this.RedisClient.send_command('ZCARD', [this.QueueName], (err, result) => {
                if(err)
                    return reject(err)

                return resolve(result)
            })
        });
    }

    deleteQueue() {
        return new Promise((resolve, reject) => {
            this.RedisClient.send_command('ZREMRANGEBYSCORE', [this.QueueName, 0, 1000], (err, result) => {
                if(err)
                    return reject(err)

                return resolve(result)
            })
        });
    }

    sortedPush(item){
        return new Promise(async (resolve, reject) => {
            try{
                this._logger.info('sorted push:', item);
                let items = await this.getQueueItems();
                if(!items || items.length == 0){
                    await this.addToQueue(item, 1)
                    return resolve()
                }else{
                    items = collect(items).map((item, key) => {
                        let item_content = item['content'];
                        item_content['_weight'] = item['weight'];
                        return item_content;
                    })
                    let items_grouped = items.sortByDesc('_weight').groupBy('BotAccount');
                    //console.log(items_grouped.all(), items.all())

                    this._logger.info(items_grouped.all(), items.all())
                    if(items_grouped.get(item['BotAccount'])){
                        let lastItem = items_grouped.get(item['BotAccount']).sortByDesc('_weight').first();
                        //console.log('last:', lastItem)
                        await this.addToQueue(item, lastItem['_weight'] + 0.000001);
                        return resolve();
                    }else{
                        let lastItem = items.sortByDesc('_weight').first();
                        this._logger.info('last_item', lastItem)
                        //console.log('last:', lastItem)
                        await this.addToQueue(item, Math.ceil(lastItem['_weight'] + 0.000001))
                        return resolve();
                    }
                }
            }catch (e) {
                this._logger.error(e)
            }
        });
    }

    _isJSON(str) {
        if (typeof str == 'string') {
            try {
                let obj = JSON.parse(str);
                return !!(typeof obj == 'object' && obj);

            } catch(e) {
                return false;
            }
        }
    }
}

module.exports = RedisPriorityQueue