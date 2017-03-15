var q = require('q'),
_ = require('lodash'),
EventEmitter = require('events').EventEmitter;

var queues = {
    users: {
        pending: 'usersPending',
        processing: 'usersProcessing',
        emitter: {
            emit(channel){
                // ('emit',channel)
                return pubsub.publish(`queue_users_${channel}`,'1')
            },
            once(channel,cb){
                // ('once',channel)
                return pubsub.once(`queue_users_${channel}`,(...a)=>{
                    // ('once',channel,...a)
                    cb(...a)
                })
            }
        }
    },
    rooms: {
        pending: 'roomsPending',
        processing: 'roomsProcessing',
        emitter: {
            emit(channel){
                // ('emit',channel)
                return pubsub.publish(`queue_rooms_${channel}`,'1')
            },
            once(channel,cb){
                // ('once',channel)
                return pubsub.once(`queue_rooms_${channel}`,(...a)=>{
                    // ('once',channel,...a)
                    cb(...a)
                })
            }
        }
    }
};

function wrap(redis,name){
    return (...a)=>q.ninvoke(redis,name,...a)
}

let pubsub
let redis = {
    funcs: ['get','del','llen','lrem','lpush','ltrim','rpoplpush']
}

module.exports = {
    wrap(nredis,npubsub){
        redis.funcs.forEach(f=>redis[f]=wrap(nredis,f))
        pubsub = npubsub
    },
    fetch(name, cb) {
        // console.log('fetch',name)
        let defer = q.defer()
        try {
            var check = function () {
                // console.log('fetchCheck',name)
                redis.rpoplpush(queues[name].pending,queues[name].processing)
                    .then(item=>{
                        // console.log('fetch',item)
                        if(!item || item == 'nil'){
                            queues[name].emitter.once('add', check);
                            // console.log('fetch waiting...')
                            return
                        }
                        defer.resolve(item)
                    }).catch(err=>console.error('fetch',err))
            };
            check();
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    },
    markDone(name, id, cb) {
        // console.log('markDone',name, id)
        let defer = q.defer()
        try {
            redis.lrem(queues[name].processing,0,id)
            queues[name].emitter.emit('done');
            defer.resolve(true);
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    },
    add(name, id, cb) {
        // console.log('add',name, id)
        let defer = q.defer()
        try {
            redis.lpush(queues[name].pending,id)
            queues[name].emitter.emit('add');
            defer.resolve(true);
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    },
    addMulti(name, array, cb) {
        // console.log('addMulti',name, array)
        let defer = q.defer()
        try {
            // console.log('lpush',name,...array)
            redis.lpush(queues[name].pending,...array)
            queues[name].emitter.emit('add');
            defer.resolve(true);
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    },
    whenAllDone(name, cb) {
        // console.log('whenAllDone',name, cb)
        let defer = q.defer()
        try {
            var check = function (reset) {
                q.all([
                    redis.llen(queues[name].pending),
                    redis.llen(queues[name].processing)
                ]).then(cnts=>{
                    // console.log(cnts,!!(cnts[0]+cnts[1]))
                    if(cnts[0] + cnts[1]){
                        queues[name].emitter.once('done', check);
                        return;    
                    }
                    pubsub.publish('queueDone:' + name, '1');
                    defer.resolve(true);
                })
            };
            check();
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    },
    reset(name, cb) {
        let defer = q.defer()
        try {
            // console.log('queue',name,'reset')
            q.all([
                redis.del(queues[name].pending),
                redis.del(queues[name].processing),
            ]).then((ret)=>{
                // console.log('queue',name,'reset','success',ret)
                queues[name].emitter.emit('done',true);
                defer.resolve(true);
            })
        }
        catch (e) {
            defer.reject(e.message);
            console.error(e);
        }
        return defer.promise
    }
};

// setInterval(() => {
//     q.all([
//         redis.get(queues.users.processing),
//         redis.get(queues.users.pending),
//     ]).then(s=>console.log(...s))
// }, 500);