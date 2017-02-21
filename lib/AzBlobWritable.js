'use strict';

const crypto = require('crypto'),
      API_VERSION = '2016-05-31' // maximum size of blocks has been increased to 100 MB
// ---------------------------------------------- 
// creates an ad-hoc SAS on the container
function createSASLocator (storageacc, container, minutes, key) {

// first construct the string-to-sign from the fields comprising the request,
// then encode the string as UTF-8 and compute the signature using the HMAC-SHA256 algorithm
// Note that fields included in the string-to-sign must be URL-decoded

    let exp_date = new Date(Date.now() + (minutes*60*1000)),
        signedpermissions = "rwdl",
        signedstart = '',
        signedexpiry= exp_date.toISOString().substring(0, 19) + 'Z',
        canonicalizedresource= `/blob/${storageacc}/${container}`,
        signedidentifier = '', //if you are associating the request with a stored access policy.
        signedIP = '',
        signedProtocol = 'https',
        signedversion = API_VERSION,
        rscc = '', // Blob Service and File Service Only, To define values for certain response headers, Cache-Control
        rscd = '', // Content-Disposition
        rsce = '', // Content-Encoding
        rscl = '', // Content-Language
        rsct = '', // Content-Type
        stringToSign = 
`${signedpermissions}
${signedstart}
${signedexpiry}
${canonicalizedresource}
${signedidentifier}
${signedIP}
${signedProtocol}
${signedversion}
${rscc}
${rscd}
${rsce}
${rscl}
${rsct}`

    const sig = crypto.createHmac('sha256', new Buffer(key, 'base64')).update(stringToSign, 'utf-8').digest('base64');

    return  {
      hostname: 
            `${storageacc}.blob.core.windows.net`,
      container:
            `${container}`,
      sas:
            `sv=${signedversion}&` +  // signed version
            "sr=c&" +   // signed resource 'c' = Container 'b' = Blob
            `sp=${signedpermissions}&` + //  The permissions associated with the shared access signature
            //    "st=2016-08-15T11:03:04Z&" +
            `se=${signedexpiry}&` + // signed expire 2017-08-15T19:03:04Z
            //    "sip=0.0.0.0-255.255.255.255&" +
            `spr=${signedProtocol}&` +
            `sig=${encodeURIComponent(sig)}`
    }
}

// Provides a FlushWritable class which is used exactly like stream.Writable but supporting a new _flush method 
// which is invoked after the consumer calls .end() but before the finish event is emitted.

const https = require('https'),
      util = require('util'),
      stream = require('stream'), 
      FlushWritable = require('flushwritable'), // replacment to stream.Writable
      BLOCK_SIZE = 1024 * 1024 * 2 // 2MB blocks

function AzBlobWritable (saslocator, fileName)  {
  if (!(this instanceof AzBlobWritable)) {
    return new AzBlobWritable(saslocator, fileName)
  } else {
    this.saslocator = saslocator
    this.fileName = fileName
    // fixed-sized, raw memory allocations outside the V8 heap, size =  bytes
    this.blockBuffer = Buffer.allocUnsafe(BLOCK_SIZE)
    this.blockBuffer_length = 0
    this.currblock = 1
    this.sentBlockIDs = []
    this.totalbytes = 0
    // Streams will store data in an internal buffer that can be retrieved using writable._writableState.getBuffer()
    // The amount of data potentially buffered depends on the highWaterMark option passed into the streams 
    // Data is buffered in Writable streams when the writable.write(chunk) method is called repeatedly. 
    // While the total size of the internal write buffer is below the threshold set by highWaterMark, calls to writable.write() will return true
    // each blob can be upto a maximum of 100 MB, but the buffer will be
    FlushWritable.call(this, {highWaterMark: BLOCK_SIZE}); // 1MB
  }
}  

util.inherits(AzBlobWritable, FlushWritable);

AzBlobWritable.prototype._write = function(source, enc, next) {
  // Callback for when this chunk of data is flushed. The return value indicates if you should continue writing right now
  // Once the callback is invoked, the stream will emit a 'drain' event
  let space_left = BLOCK_SIZE - this.blockBuffer_length,
      source_to_copy = Math.min (space_left, source.length),
      source_remaining = source.length - source_to_copy

  this.totalbytes+= source.length
  //          target, begin target offset (def:0), source start, source end (not inclusive): (def: source.length)
  source.copy(this.blockBuffer, this.blockBuffer_length, 0, source_to_copy)
  this.blockBuffer_length = this.blockBuffer_length+source_to_copy
  //console.log (`[processed ${this.totalbytes}]  copy current buffer length ${this.blockBuffer_length},  source.length ${source.length}, source copied ${source_to_copy}, source remaining ${source_remaining}`)

  if (this.blockBuffer_length < BLOCK_SIZE) { 
    // blockBuffer got space_left
    next() // send next data (add a error string if error)
  } else {
    // blockBuffer
     this._putblock (this.currblock, this.blockBuffer.slice(0, this.blockBuffer_length)).then(() => {
        this.blockBuffer_length = 0
        this.currblock++
        if (source_remaining > 0) {
          // got remainder to copy
          //console.log (`copying remaining, target start idx: ${this.blockBuffer_length},  source start idx: ${source_to_copy}, source end idx: ${source.length}`)
          source.copy(this.blockBuffer, this.blockBuffer_length, source_to_copy, source.length)
          this.blockBuffer_length = source_remaining
          //console.log (`copying remaining, buffer new length ${this.blockBuffer_length},  source.length ${source.length}, source copied ${source_remaining}`)
        }
        next() // send next data (add a error string if error)
      }, (err) => next(err))
  }
}

AzBlobWritable.prototype._flush = function(next) {
  // enabled by 'flushwritable'
  // The 'end' event is emitted after all data has been output, which occurs after the callback in _flush() has been called.
    //console.log('_flush');
    let finalBlockFn = () => {
      this._putblock ().then (() => {
        next() // send next data (add a error string if error)
      }, (err) => next(err))
    }
    if (this.blockBuffer_length >0) {
      //console.log (`writing ${this.blockBuffer_length}`)
      this._putblock (this.currblock, this.blockBuffer.slice(0, this.blockBuffer_length)).then(finalBlockFn, (err) => next(err))
    } else {
      finalBlockFn()
    }
}


//
//Each block can be a different size, up to a maximum of 100 MB, block blob can include up to 50,000 blocks.
AzBlobWritable.prototype._putblock = function (currblock, data) {
  
    return new Promise ((acc,rej) => {

        let comp
        if (currblock && data) {
            let blockid = "KH01" + ('00000'+currblock).slice(-5)
            comp = `comp=block&blockid=${new Buffer(blockid).toString('base64')}`
            this.sentBlockIDs.push(blockid);
            //console.log (`AzBlobWritable._putblock: putting block ${blockid}`)// size: ${data.length.toLocaleString()} bytes`)
        } else {
            comp = "comp=blocklist"
            data = '<?xml version="1.0" encoding="utf-8"?>' +
                    '<BlockList>' +
                    this.sentBlockIDs.map((l) => `<Latest>${new Buffer(l).toString('base64')}</Latest>`).join('') +
                    '</BlockList>'
            //console.log (`AzBlobWritable._putblock: putting blocklist ${data}`)
        }

        let reqopts = {
          hostname: this.saslocator.hostname,
          path: `/${this.saslocator.container}/${encodeURIComponent(this.fileName)}?${comp}&${this.saslocator.sas}`,
          method: 'PUT',
          headers: {
              "Content-Length": data.length,
              "x-ms-version": API_VERSION
          }
        }
        //console.log (`sending to ${JSON.stringify(reqopts,null,1)}` )
        let putreq = https.request(reqopts, (res) => {
              res.on('data', (d) => {
                  console.error (`on data ${d}`)
              });

              if(res.statusCode == 200 || res.statusCode == 201) {
                  acc()
              } else {
                  rej(res.statusCode)
              }
          }).on('error', (e) =>  rej(e));
        //console.log (`putting writing data ${data.length}`)
        putreq.write (data)
        putreq.end()
    })
}

exports.AzBlobWritable = AzBlobWritable
exports.createSASLocator = createSASLocator