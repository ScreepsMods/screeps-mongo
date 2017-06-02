const dnode = require('dnode');
const q = require('q');
const net = require('net');
const EventEmitter = require('events').EventEmitter;
const config = require('./config-manager').config;
const { MongoClient, ObjectId } = require('mongodb');
const Redis = require('redis');

config.common.dbCollections = [
    'leaderboard.power',
    'leaderboard.seasons',
    'leaderboard.world',
    'market.intents',
    'market.orders',
    'market.stats',
    'rooms',
    'rooms.objects',
    'rooms.flags',
    'rooms.intents',
    'rooms.terrain',
    'transactions',
    'users',
    'users.code',
    'users.console',
    'users.messages',
    'users.money',
    'users.notifications',
    'users.resources',
];

config.common.storage = exports;

exports.db = {};
exports.queue = {};
exports.env = {
    keys: {
        ACCESSIBLE_ROOMS: 'accessibleRooms',
        MEMORY: 'memory:',
        GAMETIME: 'gameTime',
        MAP_VIEW: 'mapView:',
        TERRAIN_DATA: 'terrainData',
        SCRIPT_CACHED_DATA: 'scriptCachedData:',
        USER_ONLINE: 'userOnline:',
        MAIN_LOOP_PAUSED: 'mainLoopPaused',
        ROOM_HISTORY: 'roomHistory:',
        ROOM_VISUAL: 'roomVisual:',
        MEMORY_SEGMENTS: 'memorySegments:',
        PUBLIC_MEMORY_SEGMENTS: 'publicMemorySegments:',
        ROOM_INTENTS: 'roomIntents:'
    }
};
exports.pubsub = {
    keys: {
        QUEUE_DONE: 'queueDone:',
        RUNTIME_RESTART: 'runtimeRestart',
        TICK_STARTED: "tickStarted",
        ROOMS_DONE: "roomsDone"
    }
};

exports._connect = function storageConnect() {

    if(exports._connected) {
        return q.when();
    }

    let uri = process.env.MONGO_URI || 'mongodb://mongo:27017/screeps'

    console.log('Connecting to mongo');

    let redis = Redis.createClient({ host: 'redis' })
    let pub = Redis.createClient({ host: 'redis' })
    let sub = Redis.createClient({ host: 'redis' })

    let mongo = q.ninvoke(MongoClient,'connect',uri)
        .then(db=>{
            console.log('Connected to mongo')
            function wrapCollection(collection,cname) {
                var wrap = {};
                ['find'].forEach(method => {
                    wrap[method] = (...a)=>{
                        if(typeof a[0] == 'object'){
                            a[0] = replaceIdsWithOid(a[0])
                        }
                        
                        let cursor = collection[method](...a)
                        let chain = q.ninvoke(cursor,'toArray');
                        chain = chain.then(d=>d.map(d=>{
                            if(d && d._id)
                                d._id = d._id.toString()
                            if(d && d.user)
                                d.user = d.user.toString()
                            return d
                        }))
                        return chain
                    }
                });
                function replaceIdsWithOid(obj,key){
                    if(!obj) return obj
                    if(typeof obj == 'string' && obj.match(/^[a-f0-9]{24}$/) && key != 'user')
                        return new ObjectId(obj)
                    if(typeof obj == 'object' || obj instanceof Array){
                        let keys = Object.keys(obj)
                        keys.forEach(k=>obj[k] = replaceIdsWithOid(obj[k],k))
                    }
                    return obj
                }
                ['findOne','by','count','ensureIndex','remove','insert','update'].forEach(method => {
                    wrap[method] = (...a)=>{
                        if(typeof a[0] == 'object'){
                            a[0] = replaceIdsWithOid(a[0])
                        }
                        let chain = q.ninvoke(collection,method,...a);
                        if(method == 'findOne')
                            chain = chain.then(d=>{
                                if(d && d._id)
                                    d._id = d._id.toString()
                                if(d && d.user)
                                    d.user = d.user.toString()
                                return d
                            })
                        return chain
                    }
                });

                function idToDb(id){
                    return id.length == 24?new ObjectId(id):id
                }
                function idFromDb(id){
                    return id.toString()
                }
                wrap.drop = (...a)=>{
                    return q.ninvoke(collection,'drop',...a).catch(e=>q.resolve())
                }
                wrap.clear = wrap.drop
                wrap.removeWhere = wrap.remove
                wrap.by = (_id)=>wrap.find({ _id })
                wrap.findEx = function(query, opts) {
                    try {
                        let chain = collection.find(query);
                        if (opts.sort) {
                            chain = chain.sort(opts.sort);
                        }
                        if (opts.offset) {
                            chain = chain.offset(opts.offset);
                        }
                        if (opts.limit) {
                            chain = chain.limit(opts.limit);
                        }
                        return q.ninvoke(chain,'toArray');
                    }
                    catch (e) {
                        // cb(e.message);
                        console.error(e);
                        return q.reject(e.message)
                    }
                }
                wrap.bulk = function(bulk, cb) {
                    // console.log('bulk',bulk)
                    let defer = q.defer()
                    try {
                        let ps = bulk.map(i => {
                            if(i.op == 'update')
                                return collection.update({ _id: i.id.length == 24?new ObjectId(i.id+''):i.id }, { $set: i.$set })
                            if(i.op == 'insert')
                                return collection.insert(i.data)
                            if(i.op == 'remove')
                                return collection.remove({ _id: i.id.length == 24?new ObjectId(i.id+''):i.id })
                            if(i.op == 'inc')
                                return collection.update({ _id: i.id.length == 24?new ObjectId(i.id+''):i.id },{ $inc: { [i.key]: i.amount } })
                            console.error('UNKNOWN BULK!',i)
                        });
                        return q.all(ps)
                    }
                    catch (e) {
                        cb && cb(e.message);
                        console.error(e);
                        return q.reject(e.message)
                    }
                }
                return wrap;
            }
            config.common.dbCollections.forEach(i=>exports.db[i] = wrapCollection(db.collection(i),i))

            return exports.db.rooms.count().then(count=>{
                if(!count){
                    console.log('Importing DB')
                    let olddb = require('/screeps/db.json')
                    let ps = olddb.collections.map(oldcol=>{
                        let name = oldcol.name
                        console.log(name)
                        if(name == 'env'){
                            return
                            let p = oldcol.data.map(row=>{
                                let ps = []
                                for(let k in row.data){
                                    let v = row.data[k]
                                    // console.log(k,v)
                                    ps.push(q.ninvoke(redis,'set',k,typeof v == 'object'?JSON.stringify(v):v))
                                }
                                return q.all(ps)
                            })
                            return q.all(p)   
                        }else{
                            let col = exports.db[name]
                            let p = q.all(oldcol.data.map(row=>{
                                delete row.meta
                                delete row.$loki
                                return col.insert(row)
                            }))
                            return q.all(p)
                        }
                    })
                    return q.all(ps).catch(err=>console.error(err))
                }
            })
        })

    Object.assign(exports.pubsub,{
        ee: new EventEmitter(),
        subscribed: {},
        publish(channel,data){
            pub.publish(channel,data)
            return q.when()
        },
        subscribe(channel,cb){
            if(!this.subscribed[channel]){
                if(channel.match(/[?*]/))
                    sub.psubscribe(channel)
                else
                    sub.subscribe(channel)
                this.subscribed[channel] = true
            }
            this.ee.on(channel,cb)
            return q.when()
        },
        once(channel,cb){
            if(!this.subscribed[channel]){
                if(channel.match(/[?*]/))
                    sub.psubscribe(channel)
                else
                    sub.subscribe(channel)
                this.subscribed[channel] = true
            }
            this.ee.once(channel,cb)
            return q.when()   
        }
    })
    sub.on('message',(channel,message)=>{
        exports.pubsub.ee.emit(channel,message)
    })
    sub.on('pmessage',(pattern,channel,message)=>{
        exports.pubsub.ee.emit(channel,message)
        exports.pubsub.ee.emit(pattern,channel,message)
        // console.log(pattern,channel)
    })

    Object.assign(exports.env, {
        get: (...a)=>q.ninvoke(redis,'get',...a).catch(err=>(console.error('get',err),err)),
        mget: (...a)=>q.ninvoke(redis,'mget',...a).catch(err=>(console.error('mget',err),err)),
        set: (...a)=>q.ninvoke(redis,'set',...a).catch(err=>(console.error('set',err),err)),
        setex: (...a)=>q.ninvoke(redis,'setex',...a).catch(err=>(console.error('setex',err),err)),
        expire: (...a)=>q.ninvoke(redis,'expire',...a).catch(err=>(console.error('expire',err),err)),
        ttl: (...a)=>q.ninvoke(redis,'ttl',...a).catch(err=>(console.error('ttl',err),err)),
        del: (...a)=>q.ninvoke(redis,'del',...a).catch(err=>(console.error('del',err),err)),
        hmget: (...a)=>q.ninvoke(redis,'hmget',...a).catch(err=>(console.error('hmget',err),err)),
        hmset: (...a)=>q.ninvoke(redis,'hmset',...a).catch(err=>(console.error('hmset',err),err)),
        hget: (...a)=>q.ninvoke(redis,'hget',...a).catch(err=>(console.error('hget',err),err)),
        hset: (...a)=>q.ninvoke(redis,'hset',...a).catch(err=>(console.error('hset',err),err)),
        hgetall: (...a)=>q.ninvoke(redis,'hgetall',...a).catch(err=>(console.error('hgetall',err),err)),
    });

    exports._connected = true;
    exports.resetAllData = () => q.when(); // Temp dummy

    Object.assign(exports.queue,require('./queue'));
    exports.queue.wrap(redis,exports.pubsub)

    return mongo;
};