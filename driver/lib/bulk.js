var q = require('q'),
    _ = require('lodash'),
    common = require('../../common'),
    ObjectId = require('mongodb').ObjectID;


function removeHidden(obj) {
    for(var i in obj) {
        if(i[0] == '_') {
            delete obj[i];
            continue;
        }
        if(_.isArray(obj[i])) {
            obj[i].forEach(removeHidden);
            continue;
        }
        if(_.isObject(obj[i])) {
            removeHidden(obj[i]);
        }
    }
}

module.exports = function(collectionName) {

    var bulk = [],
        opsCnt = 0,
        updates = {};

    return {
        update(id, data) {
            if(!id) {
                return;
            }
            if(_.isObject(id) && id.toString().length == 24)
                id = id.toString()
            opsCnt++;
            data = _.cloneDeep(data);

            _.forEach(data, (value, key) => {
                if(_.isObject(value) || id instanceof ObjectId) {
                    if (!_.isObject(id)) {
                        throw new Error(`can not update an object diff property '${key}' without object reference`);
                    }
                    var originalValue = id[key] || {};
                    _.merge(originalValue, value);
                    data[key] = originalValue;
                }
            });
            // console.log(id,id.toString().length,id instanceof ObjectId,ObjectId)
            if(_.isObject(id) && !(id instanceof ObjectId)) {
                _.merge(id, data);
                id = id._id;
            }

            removeHidden(data);

            updates[id] = updates[id] || {};
            _.extend(updates[id], data);
        },
        insert(data, id) {
            data = _.cloneDeep(data);
            removeHidden(data);

            if(id) {
                data._id = id;
            }

            opsCnt++;
            bulk.push({op: 'insert', data});
        },
        remove(id) {
            if(!id) {
                return;
            }
            opsCnt++;
            bulk.push({op: 'remove', id});
        },
        inc(id, key, amount) {
            if(!id) {
                return;
            }
            if (_.isObject(id)) {
                id = id._id;
            }
            opsCnt++;
            bulk.push({op: 'inc', id, key, amount});
        },
        execute() {
            if(!opsCnt) return q.when({});
            for(var id in updates) {
                bulk.push({op: 'update', id, $set: updates[id]});
            }
            return common.storage.db[collectionName].bulk(bulk);
        }
    }

};