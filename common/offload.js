const Redis = require('redis');
let sub = Redis.createClient({ host: 'redis' })
const common = require('./index')
const storage = common.storage
const { db, env, pubsub } = storage

common.configManager.config.common.dbCollections.push('rooms.history')

storage._connect().then(()=>{
  pubsub.subscribe('uploadHistory',(raw)=>{
    let { roomId, baseTime, data } = JSON.parse(raw)
    if(!data || !data[""+baseTime]) {
      return;
    }

    let curTick = baseTime
    let curObjects = JSON.parse(data[""+baseTime])
    let result = {
      timestamp: Date.now(),
      room: roomId,
      base: curTick,
      ticks: {
        [curTick]: curObjects
      }
    };

    curTick++;
    while(data[""+curTick]) {
      var objects = JSON.parse(data[""+curTick]);
      var diff = common.getDiff(curObjects, objects);
      result.ticks[curTick] = diff;
      curObjects = objects;
      curTick++;
    }
    db['rooms.history'].insert(result)
    // config.engine.emit('saveRoomHistory',roomId, baseTime, result);
  })
})