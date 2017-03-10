const https = require('https')


module.exports = AzListBlobs =  (saslocator, prefix, bnames, marker) => {
  return new Promise((acc, re) => {
    https.get(`https://${saslocator.hostname}/${saslocator.container}?restype=container&comp=list&prefix=${prefix}` + (marker? `&marker=${marker}&` : '&') + saslocator.sas, (res) => {

      res.setEncoding('utf8');
      let rawData = '', code = 200;

      if(res.statusCode == 200 || res.statusCode == 201) {
          //console.log(res.statusCode)
      } else {
        code = res.statusCode
        console.log(`error returned ${res.statusCode}`)
      }
      res.on('data', (chunk) => {
        if (code != 200) {
          console.log (`error data : ${chunk}`)
        }
        rawData += chunk
      });
      res.on('error', (d) => {
          console.error (`error on data ${d}`)
      });
      res.on('end', () => {
          try {
            let sidx = rawData.indexOf('<Blobs>'), eidx = rawData.indexOf('</Blobs>'), blobs = rawData.substring(sidx+7,eidx),
                smidx = rawData.indexOf('<NextMarker>'), emidx = rawData.indexOf('</NextMarker>'), marker = emidx > 0 && rawData.substring(smidx+12,emidx),
                blobsarr = blobs.split('<Blob>').slice(1), 
                new_bnames = blobsarr.map ((b) => b.substring(b.indexOf('<Name>')+6,b.indexOf('</Name>')))
            //console.log (`adding ${new_bnames.length} blob names, marker ${(marker != false)}`)
            process.stdout.write('.')
            let ret = bnames.concat (new_bnames)
            if (marker) {
              AzListBlobs (saslocator, prefix, ret, marker).then((succ) => { return acc(succ)})
            } else {
              return acc(ret)
            }
          } catch (e) {
              console.log(e.message);
          }
      })
    })
  })
}