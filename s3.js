const csv = require("fast-csv")
      s3 = require('s3'),
      https = require('https'),
      fs = require('fs'),
      logerr = fs.openSync(`log-${new Date().getTime()}.csv`, 'a'),
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
    plimit = promiseLimit(10), pall = [],
    si = setInterval (() => { console.log (`batches = ${batches} queued = ${queued}, processing = ${processing}, error = ${error}, complete = ${complete}`)}, 2000)


let streamBlob = (s3blob, azblob, key) => {
    return new Promise((accept, reject) => {
        processing++
        csv
          .fromStream(s3auth.downloadStream(s3blob), {headers: false,  delimiter: '|'})
          .pipe(csv.createWriteStream({headers: false}))
          .pipe(azblob)

        azblob.on('finish', () => { 
           
          let payload =  JSON.stringify({
                "data": {
                  "baseType": "OpenSchemaData",
                  "baseData": {
                    "ver": "2",
                    "blobSasUri": `https://${saslocator.hostname}/${saslocator.container}/${encodeURIComponent(key)}?${saslocator.sas}`,
                    "sourceName": "a8f38c51-6036-4d99-8f6e-e862a1fed116",
                    "sourceVersion": "1.0"
                  }
                },
                "ver": 1,
                "name": "Microsoft.ApplicationInsights.OpenSchema",
                "time": new Date().toISOString(),
                "iKey": "808c7ad8-99b1-4737-91fe-0f63ca17a75a"
              }),
              putreq = https.request({
                hostname: 'dc.services.visualstudio.com',
                path: '/v2/track',
                method: 'POST',
                headers: {
                  "Content-Length": payload.length
                }
              }, (res) => {

                if(res.statusCode == 200 || res.statusCode == 201) {
                  processing--; complete++
                  fs.writeSync(logerr, `success,${key},AppInsights,\n`)
                  accept(key) 
                } else {
                  processing--; error++
                  fs.writeSync(logerr, `error,${key},AppInsights,${res.statusCode}\n`)
                  reject(`failed code from AppInsights for ${key} - ${res.statusCode}`)
                }
              }).on('error', (e) => {
                fs.writeSync(logerr, `error,${key},AppInsights,${e}\n`)
                reject(`failed to send to AppInsights key  ${key} - ${e}`)
              })

          putreq.write (payload)
          putreq.end()
        })
        azblob.on('error', (e) => { 
          processing--; error++; 
          fs.writeSync(logerr, `error,${key},Pipe,${e}\n`)
          reject(e)
        })
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
    Promise.all(pall).then(() => {
      clearInterval(si)
      console.log (`batches = ${batches} queued = ${queued}, processing = ${processing}, error = ${error}, complete = ${complete}`)
      console.log ('done')
      
    }, (e) => console.error (`error ${e}`))
})
