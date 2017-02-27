

function PromiseRunner(limit = 15) {
  if (!(this instanceof PromiseRunner)) {
    return new PromiseRunner(limit)
  }
  this.limit = limit
  this.pfnbuffer = []
  this.pstarted = []
  this.nexttorun = 0
  this.pdone = 0
  this.perror = 0

  this.debug = false
  if (this.debug) {
    this.debug_int = setInterval (this.logmetrics.bind(this), 1000)
  }
}

PromiseRunner.prototype.logmetrics = function(last)  {
  console.log (`running=${this.pstarted.length-this.pdone}  (pdone=${this.pdone} errors=${this.perror} bufferlength=${this.pfnbuffer.length} nexttorun=${this.nexttorun}) ${last == true && 'last' || ''}`)
}

PromiseRunner.prototype.done = function() {
  // call this when no more promises will be submitted
  return new Promise ((accept,reject) => {

    let started = this.pstarted.length,
        checkend = () => {
          if (this.pstarted.length > started) {
            started = this.pstarted.length
            Promise.all(this.pstarted).then (checkend, checkend)
          } else {
            if (this.debug) {
              clearInterval(this.debug_int); 
              this.logmetrics(true) 
            }
            if (this.perror>0) {
              reject()
            } else {
              accept()
            }
          }
        }
    Promise.all(this.pstarted).then(checkend, checkend)   
  })
}

PromiseRunner.prototype.promiseFn = function(pfn) {
    this.pfnbuffer.push(pfn)

    let checkRunNext = () => {
      if (this.nexttorun < this.pfnbuffer.length && this.nexttorun - this.pdone < this.limit) {
            this.pstarted.push(this.pfnbuffer[this.nexttorun++]().then(donefn, errfn))
      }
    }

    let errfn = () => {
        this.pdone++
        this.perror++
        checkRunNext()
    }
    let donefn = () => {
        this.pdone++
        checkRunNext()
    }
    checkRunNext()
}

module.exports = PromiseRunner