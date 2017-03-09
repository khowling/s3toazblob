const 
  //csv = require("fast-csv")
  s3 = require('s3'),
  https = require('https'),
  PromiseRunner = require('./lib/PromiseRunner'),
  { createSASLocator, AzBlobWritable } = require('./lib/AzBlobWritable'),
  ChangeDelimiter = require('./lib/ChangeDelimiter'),
  AzListBlobs = require('./lib/AzListBlobs')

if (!(process.env.ACCESSKEYID && process.env.SECRETACCESSKEY && process.env.BUCKET && process.env.STORAGEACC && process.env.CONTAINER && process.env.KEY)) {
    console.log ('set environment:\n\nexport ACCESSKEYID=""\nexport SECRETACCESSKEY=""\nexport BUCKET=""\nexport STORAGEACC=""\nexport CONTAINER=""\nexport KEY=""\n')
    process.exit (1)
}

let args = process.argv.slice(2),
    mode = 'all'

if (args[0] && args[0].startsWith('-')) {
  switch (args[0]) {
    case '-i': 
      mode = 'ionly'; break;
    default: 
      console.log (`unknown flag ${args[0]}`)
      process.exit (1)
  }
  args.shift()
}

 PREFIX = args[0] || process.env.PREFIX
if (!PREFIX) {
    console.log ('pass S3 Prefix on command line or set PREFIX env')
    process.exit (1)
}

let batches = 0, queued = 0, skipped =0, processing = 0, error = 0, complete = 0

let sendToAppInsights = (key) => {
  return new Promise((accept, reject) => {
    let payload =  JSON.stringify({
        "data": {
          "baseType": "OpenSchemaData",
          "baseData": {
            "ver": "2",
            "blobSasUri": `https://${saslocator.hostname}/${saslocator.container}/${encodeURIComponent(key)}?${saslocator.sas}`,
            "sourceName": process.env.APPINSIGHTS_SOURCENAME,
            "sourceVersion": "1.0"
          }
        },
        "ver": 1,
        "name": "Microsoft.ApplicationInsights.OpenSchema",
        "time": new Date().toISOString(),
        "iKey": process.env.APPINSIGHTS_IKEY

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
            accept(key) 
        } else {
            reject(`AppInsights ReturnCode,${res.statusCode}`)
        }
      }).on('error', (e) => {
        reject(`AppInsights Error,${e}`)
      })
    putreq.write (payload)
    putreq.end()
  })
}


const saslocator = createSASLocator(process.env.STORAGEACC, process.env.CONTAINER, 3000, process.env.KEY)

if (mode == 'all') {

  const s3auth = s3.createClient({s3Options: { accessKeyId: process.env.ACCESSKEYID, secretAccessKey: process.env.SECRETACCESSKEY}}),
        plimit = new PromiseRunner(10),
        fs = require('fs'),
        startsec =  Math.round(new Date().getTime()/1000),
        logerr = fs.openSync(`log-${startsec}.csv`, 'a')

  
  let streamBlob = (s3blob, azblob, key) => {
      return new Promise((accept, reject) => {
          processing++
          s3auth.downloadStream(s3blob).pipe(new ChangeDelimiter()).pipe(azblob)

          azblob.on('finish', () => { 
            if (!(process.env.APPINSIGHTS_SOURCENAME && process.env.APPINSIGHTS_IKEY)) {
              processing--; complete++
              fs.write(logerr, `success,${key},AzBLob,\n`, () => {
                accept(key) 
              })
            } else {
              sendToAppInsights(key).then((succkey) => {
                processing--; complete++
                fs.write(logerr, `success,${key},AppInsights,\n`, () => {
                  accept(key) 
                })
              }, (err) => {
                processing--; error++
                fs.write(logerr, `error,${key},${err}\n`, () => {
                    reject(`Error: key ${key} - ${err}`)
                })
              })
            }
          })
          azblob.on('error', (e) => { 
            processing--; error++; 
            fs.write(logerr, `error,${key},Pipe,${e}\n`, () => { reject(e) })
          })
      })
  }

  let azBlobs = [], nameSet, skip = (process.env.SKIP_KEY != null)
    
  process.stdout.write(`Getting Azore blobs for ${PREFIX}: `)
  AzListBlobs(saslocator, PREFIX, azBlobs). then ((succ) => {

    process.stdout.write (` ${succ.length} Azure blobs\n`)
    nameSet = new Set(succ)

            
    const mgfn = () => {console.log (`skipped = ${skipped} queued = ${queued}, processing = ${processing}, error = ${error}, complete = ${complete} rate = ${complete/((Math.round(new Date().getTime()/1000))-startsec)} files/s`) }
    console.log (`transfering files from s3 to azure with prefix "${PREFIX}"...`)
    let si = setInterval (mgfn, 4000)

    s3auth.listObjects({s3Params: {Bucket: process.env.BUCKET, Prefix: PREFIX}})
      .addListener('data', (d) => {
        batches++
        for (let f of d.Contents) {
          let key = f.Key

          if ((!skip) && (!nameSet.has(key))) {
            plimit.promiseFn(() => streamBlob ({Bucket: process.env.BUCKET, Key: key}, new AzBlobWritable(saslocator, key), key))
            queued++
          } else {
            skipped++
          }
          if (skip == true && key == process.env.SKIP_KEY) { skip = false }
        }

      })
      .addListener('end', () => {
        plimit.done().then(() => {
          clearInterval(si)
          mgfn()
        }, (e) => {
          clearInterval(si)
          mgfn()
          console.log ('** WITH ERRORS **')
        })
      })
    })
} else if (mode == 'ionly') {
  console.log (`importing existing blob into App Insights "${PREFIX}"..`)
   sendToAppInsights(PREFIX).then((succkey) => {
      console.log (`success ${PREFIX}`) 
      }, (err) => {
      console.log (`error ${PREFIX} - ${err}`)
    })
}