var q = require('q'),
    _ = require('lodash'),
    common = require('../../common'),
    config = common.configManager.config,
    env = common.storage.env;

exports.saveTick = function(roomId, gameTime, data) {
    return env.hmset(env.keys.ROOM_HISTORY + roomId, {[gameTime]: data});
};

exports.upload = function(roomId, baseTime) {
    return env.hgetall(env.keys.ROOM_HISTORY + roomId)
        .then(data => {
            process.nextTick(()=>common.storage.pubsub.publish('uploadHistory',JSON.stringify({ roomId, baseTime, data })))
            return env.del(env.keys.ROOM_HISTORY + roomId);
        })
};
