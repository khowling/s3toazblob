const 
  ungzip = require('zlib').createGunzip(),
  s3 = require('s3'),
  https = require('https'),
  PromiseRunner = require('./lib/PromiseRunner'),
  { createSASLocator, AzBlobWritable } = require('./lib/AzBlobWritable'),
  ChangeDelimiter = require('./lib/ChangeDelimiter'),
  AzListBlobs = require('./lib/AzListBlobs'),
  appInsights = require('applicationinsights')


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

if (!(process.env.APPINSIGHTS_IKEY && process.env.APPINSIGHTS_SOURCENAME) || (mode == 'all' && !(process.env.ACCESSKEYID && process.env.SECRETACCESSKEY && process.env.BUCKET && process.env.STORAGEACC && process.env.CONTAINER && process.env.KEY))) {
    console.log ('set environment:\n\nexport ACCESSKEYID=""\nexport SECRETACCESSKEY=""\nexport BUCKET=""\nexport STORAGEACC=""\nexport CONTAINER=""\nexport KEY=""\nexport APPINSIGHTS_IKEY=""\nexport KEY=""\nexport APPINSIGHTS_SOURCENAME=""\nexport KEY=""\n')
    process.exit (1)
}

let prefix = args[0] || process.env.PREFIX
if (!prefix) {
    console.log ('pass S3 Prefix on command line or set PREFIX env')
    process.exit (1)
} else {
  let date2hrsago = new Date()
  date2hrsago.setHours(date2hrsago.getHours() - 2)
  prefix = prefix.replace ('<yyyy>', date2hrsago.getFullYear()).replace('<mm>',date2hrsago.getMonth()+1).replace('<dd>',date2hrsago.getDate())
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
        let data = '';
        res.on('data',  (chunk) => { data+= chunk })
        res.on('end', () => {
            if(res.statusCode == 200 || res.statusCode == 201) {
            accept(key) 
          } else {
            reject(`AppInsights ReturnCode,${res.statusCode} ${data}`)
          }
        })
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

  //appInsights.setup(process.env.APPINSIGHTS_IKEY).setAutoCollectRequests(false).start()
  const aiclient = appInsights.getClient(process.env.APPINSIGHTS_IKEY)

  let streamBlob = (s3blob, azblob, key) => {
      return new Promise((accept, reject) => {
          processing++
          s3auth.downloadStream(s3blob).pipe(ungzip).pipe(new ChangeDelimiter()).pipe(azblob)

          azblob.on('finish', () => {
            aiclient.trackEvent("s3toblobcopy_finish", {path: key});
            if (!(process.env.APPINSIGHTS_SOURCENAME && process.env.APPINSIGHTS_IKEY)) {
              processing--; complete++
              fs.write(logerr, `success,${key},AzBLob,\n`, () => {
                accept(key) 
              })
            } else {
              sendToAppInsights(key).then((succkey) => {
                processing--; complete++
                aiclient.trackEvent("blobtoappinsights_finish", {path: key});
                fs.write(logerr, `success,${key},AppInsights,\n`, () => {
                  accept(key) 
                })
              }, (err) => {
                processing--; error++
                aiclient.trackException(`AppInsights error : ${key} : ${err}`);
                fs.write(logerr, `error,${key},${err}\n`, () => {
                    reject(`Error: key ${key} - ${err}`)
                })
              })
            }
          })
          azblob.on('error', (e) => { 
            processing--; error++; 
            aiclient.trackException(new Error(`copy error : ${key} : ${e}`));
            fs.write(logerr, `error,${key},Pipe,${e}\n`, () => { reject(e) })
          })
      })
  }

  let azBlobs = [], nameSet, skip = (process.env.SKIP_KEY != null)
    
  process.stdout.write(`Getting Azore blobs for ${prefix}: `)
  AzListBlobs(saslocator, prefix, azBlobs). then ((succ) => {

    process.stdout.write (` ${succ.length} Azure blobs\n`)
    nameSet = new Set(succ)

            
    const mgfn = () => {
      aiclient.trackMetric("queued", queued);
      aiclient.trackMetric("processing", processing);
      aiclient.trackMetric("error", error);
      aiclient.trackMetric("complete", complete);
      console.log (`skipped = ${skipped} queued = ${queued}, processing = ${processing}, error = ${error}, complete = ${complete} rate = ${complete/((Math.round(new Date().getTime()/1000))-startsec)} files/s`) 
    }
    console.log (`transfering files from s3 to azure with prefix "${prefix}"...`)
    let si = setInterval (mgfn, 4000)

    s3auth.listObjects({s3Params: {Bucket: process.env.BUCKET, Prefix: prefix}})
      .addListener('data', (d) => {
        batches++
        for (let f of d.Contents) {
          let key = f.Key,
              keyrmgz = key.replace(/\.[^.]+$/,'')

          if ((!skip) && (!nameSet.has(key) && !nameSet.has(keyrmgz))) {
            plimit.promiseFn(() => streamBlob ({Bucket: process.env.BUCKET, Key: key}, new AzBlobWritable(saslocator, keyrmgz), keyrmgz))
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
          console.log ('finished')
        }, (e) => {
          clearInterval(si)
          mgfn()
          console.log ('finished WITH ERRORS')
        })
      })
    })
} else if (mode == 'ionly') {
  console.log (`importing existing blob into App Insights "${prefix}"..`)
   sendToAppInsights(prefix).then((succkey) => {
      console.log (`success ${prefix}`) 
      }, (err) => {
      console.log (`error ${prefix} - ${err}`)
    })
}