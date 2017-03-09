

const AzListBlobs = require('./lib/AzListBlobs'),
      { createSASLocator, AzBlobWritable } = require('./lib/AzBlobWritable'),
      saslocator = createSASLocator(process.env.STORAGEACC, process.env.CONTAINER, 3000, process.env.KEY)
      


const PREFIX = process.argv[2]
let azBlobs = [], nameSet

process.stdout.write(`Getting Azore blobs for ${PREFIX}: `)
AzListBlobs(saslocator, PREFIX, azBlobs). then ((succ) => {
  process.stdout.write (` ${succ.length} Azure blobs\n`)
  nameSet = new Set(succ)

  const      
    s3 = require('s3'),
    s3auth = s3.createClient({s3Options: { accessKeyId: process.env.ACCESSKEYID, secretAccessKey: process.env.SECRETACCESSKEY}})

  let found = 0, newb = 0
  process.stdout.write(`Getting s3 blobs for ${PREFIX}: `)
  s3auth.listObjects({s3Params: {Bucket: process.env.BUCKET, Prefix: PREFIX}})
  .addListener('data', (d) => { 
      process.stdout.write('.')
      for (let f of d.Contents) {
        //console.log (`checking : ${f.Key} - ${nameSet.values().next().value}`)
        if (nameSet.has(f.Key)) {
          found++
        } else {
          newb++
        }
        //console.log(`${f.Size} -  ${f.Key}`)
        //s3auth.downloadStream({Bucket: process.env.BUCKET, Key: f.Key}).pipe(new ChangeDelimiter()).pipe(process.stdout)
        //s3auth.downloadStream({Bucket: process.env.BUCKET, Key: f.Key}).pipe(process.stdout)
      }
  })
  .addListener('end', () => {
    process.stdout.write (` ${found+newb} s3 blobs\n`)
    console.log (`Already in azure ${found}, new s3 blob ${newb}`)
  })

})


/* TEST Delimiter */
/*
const ChangeDelimiter = require('./lib/ChangeDelimiter')


fs.createReadStream(process.argv[2]).pipe(new require('./lib/ChangeDelimiter')()).pipe(process.stdout)
*/


/* TEST - promise generator */
/*
let plimit = new PromiseRunner(50), generated=0
console.log ('---start production')
var i = setInterval (() => {
  generated++
  plimit.promiseFn(() => { return new Promise((accept, reject) => {
    setTimeout(() => { accept() }, 1000)
  })})
}, 10)

setTimeout(() => {
  clearInterval(i)
  console.log (`---stopped production: ${generated}`)
  plimit.done().then((a) => { 
    console.log ('done success')
  }, (e) => console.log ('done with error') )
}, 5000)
*/