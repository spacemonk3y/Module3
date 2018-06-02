const async         = require('async');
const mongodb       = require('mongodb')
let customerdata    = require('./m3-customer-data.json')
let customeraddress = require('./m3-customer-address-data.json')

let start = 0
let limit = process.argv[2]
if(!limit) limit = 100

const url = 'mongodb://localhost:27017/'

mongodb.MongoClient.connect(url, (error, client) => {
	
	const db = client.db('edx-module2')
	let tasks = []
	let lastindex = customerdata.length-1
	
	customerdata.forEach((data, index, list)=> {
		customerdata[index] = Object.assign(data, customeraddress[index])
		if((index % limit == 0 || index==lastindex)) {
				
			let start = (index==lastindex) ? index - (index % limit) : index - limit;
			tasks.push ((callback)=>{
				db.collection('customers').insert(customerdata.slice(start, index), (error, results) => {
				  if (error) console.log(error)
				})
			    callback()
			})
		}
	})
	
	async.parallel(tasks,
	(err, results) => {
		client.close()
	});
})
