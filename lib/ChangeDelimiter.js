
const Transform = require('stream').Transform,
     util = require('util')
     fs = require('fs')


function ChangeDelimiter(options) {
  if (!(this instanceof ChangeDelimiter)) {
    return new ChangeDelimiter(options)
  }
  this.lines = 0
  Transform.call(this, options);
}

util.inherits(ChangeDelimiter, Transform);

ChangeDelimiter.prototype._swpdelimiter = function(newline) {
    let cs = newline.split('|'), 
        lastci = cs.length -1,
        ret = ''

    for (let ci = 0; ci < cs.length; ci++) {
        ret+= (cs[ci].indexOf(',') >=0 ? `"${cs[ci]}"` : cs[ci]) 
        if (ci != lastci) {
            ret+= ','
        }
    }
    //console.log (`# processed lines with columns:  ${cs.length}`)
    return ret+= '\n'
}

ChangeDelimiter.prototype._transform = function (data, encoding, callback) {

    // callback function must be called only when the current chunk is completely consumed.
    // first arg is a error, second argument is passed to the callback, 
    // it will be forwarded on to the readable.push() = this.push(data) - data = transformed chunk data)

    let xforml = '', 
        lns =  data.toString('ascii').split('\n')
    //console.log (`# new chunk with ${lns.length} lines`)
    for (let ri = 0;  ri < lns.length; ri++) {
        let l
        if (ri == 0 && this.remainder) {
            l = this.remainder + lns[ri]
        } else {
            l = lns[ri]
        }

        if (ri == lns.length -1) {
            //last line, if it has data, its not complete (no newline)
            this.remainder = l
        } else {
            xforml+= this._swpdelimiter (l)
        }
    }
    return callback(null, xforml)
};


ChangeDelimiter.prototype._flush = function(cb) {
    if (this.remainder) {
        //console.log (`# _flush with remainder : ${this.remainder}`)
        this.push(this._swpdelimiter (this.remainder))
    } else {
        this.push('\n')
    }
    return cb(null)
};

module.exports = ChangeDelimiter
