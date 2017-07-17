var q = require('q'),
    _ = require('lodash'),
    crypto = require('crypto'),
    common = require('../../common'),
    env = common.storage.env;

exports.genToken = function (id) {
    var token = crypto.createHmac('sha1', 'hsdhweh342sdbj34e').update(new Date().getTime() + id.toString()).digest('hex');
    return env.setex(`auth_${token}`, 600, id.toString())
        .then(() => token);
};

exports.checkToken = function (token, noConsume) {

    var authKey = `auth_${token}`;

    return env.get(authKey)
    .then((data) => {
        if (!data) {
            return q.reject(false);
        }

        if (!noConsume) {
            env.ttl(authKey)
            .then((ttl) => {
                if (ttl > 1000) {
                    env.expire(authKey, 600);
                }
            });
        }
        // console.log('checkToken1',token,data)
        return common.storage.db.users.findOne({ _id: data})
    })
    .then((user) => {
        // console.log('checkToken2',token,user)
        if (!user) {
            return q.reject(false);
        }
        env.set(env.keys.USER_ONLINE+user._id.toString(), Date.now());
        return user;
    });

};
