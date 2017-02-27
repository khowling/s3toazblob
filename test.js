
/* TEST s3 list */
/*
const      
  s3 = require('s3'),
  s3auth = s3.createClient({s3Options: { accessKeyId: process.env.ACCESSKEYID, secretAccessKey: process.env.SECRETACCESSKEY}})

s3auth.listObjects({s3Params: {Bucket: process.argv[2], Prefix: process.argv[3]}})
.addListener('data', (d) => { 
    for (let f of d.Contents) {
      console.log(`${f.Size} -  ${f.Key}`)
    }
})
*/

/* TEST Delimiter */
/*
const cd = require('./lib/ChangeDelimiter')
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