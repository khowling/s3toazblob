const csv = require("fast-csv")
      s3 = require('s3'),
      promiseLimit = require('promise-limit'),
      { createSASLocator, AzBlobWritable } = require('./lib/AzBlobWritable.js')

if (!(process.env.ACCESSKEYID && process.env.SECRETACCESSKEY && process.env.BUCKET && process.env.STORAGEACC && process.env.CONTAINER && process.env.KEY)) {
    console.log ('set environment:\n\nexport ACCESSKEYID=""\nexport SECRETACCESSKEY=""\nexport BUCKET=""\nexport STORAGEACC=""\nexport CONTAINER=""\nexport KEY=""\n')
    process.abort ()
}

var   PREFIX = process.argv[2] || process.env.PREFIX
if (!PREFIX) {
    console.log ('pass S3 Prefix on command line or set PREFIX env')
    process.abort ()
}

var s3auth = s3.createClient({s3Options: { accessKeyId: process.env.ACCESSKEYID, secretAccessKey: process.env.SECRETACCESSKEY}}),
    saslocator = createSASLocator(process.env.STORAGEACC, process.env.CONTAINER, 300, process.env.KEY)

let batches = 0, queued = 0, processing = 0, error = 0, complete = 0,
    plimit = promiseLimit(3), pall = []

setInterval (() => { console.log (`batches = ${batches} queued = ${queued}, processing = ${processing}, error = ${error}, complete = ${complete}`)}, 2000)


let streamBlob = (s3blob, azblob, key) => {
    return new Promise((accept, reject) => {
        processing++
        csv
        .fromStream(s3auth.downloadStream(s3blob), {headers: false,  delimiter: '|'})
        .pipe(csv.createWriteStream({headers: false}))
        .pipe(azblob)
        .on('unpipe', () => { processing--; complete++; accept(key) })
        .on('error', (e) => { processing--; error++; reject(e) })
    })
}



s3auth.listObjects({s3Params: {Bucket: process.env.BUCKET, Prefix: PREFIX}})
.addListener('data', (d) => { 
    batches++
    for (let f of d.Contents) {
        let key = f.Key
        pall.push(plimit(() => streamBlob ({Bucket: process.env.BUCKET, Key: key}, new AzBlobWritable(saslocator, key), key)))
        queued++
    }
 })
 .addListener('end', () => {
    Promise.all(pall).then(() => console.log ('done'), (e) => console.error (`error ${e}`))
})
