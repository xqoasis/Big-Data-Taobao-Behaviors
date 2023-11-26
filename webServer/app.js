'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

hclient.table('xqoasis_hw52_Spark_AirportYear_Delays').scan(
	{
		filter: {
			type: "PrefixFilter",
			value: "ORD"
		},
		maxVersions: 1
	},
	function (err, cells) {
		console.info(cells);
		console.info(groupByYear("ORD", cells));
	})

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) !== 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function weather_delay(totals, weather) {
	console.info(totals);
	let flights = totals[weather + "_flights"];
	let delays = totals[weather + "_delays"];
	if(flights === 0)
		return " - ";
	return (delays/flights).toFixed(1); /* One decimal place */
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}

function groupByYear(airport, cells) {
	// Mustache expects averages, not total flights and delays
	function yearTotalsToYearAverages(year, yearTotals) {
		let yearRow = { year : year };
		for (const weather of ['clear', 'fog', 'rain', 'snow', 'hail', 'thunder', 'tornado']) {
			yearRow[weather + '_dly'] = weather_delay(yearTotals, weather);
		}
		return yearRow;
	}
	let result = []; // let is a replacement for var that fixes some technical issues
	let yearTotals; // Flights and delays for each year
	let lastYear = 0; // No year yet
	cells.forEach(function (cell) {
		let year = Number(removePrefix(cell['key'], airport));
		if(lastYear !== year) {
			if(yearTotals) {
				result.push(yearTotalsToYearAverages(year, yearTotals))
			}
			yearTotals = {}
		}
		yearTotals[removePrefix(cell['column'], 'delay:')] = Number(cell['$'])
		lastYear = year;
	})
	return result;
}


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const airport=req.query['airport'];
    console.log(airport);
	hclient.table('annual_airport_weather_delays').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: airport
			},
			maxVersions: 1
		},
		function (err, cells) {
			let template = filesystem.readFileSync("result.mustache").toString();
			let input = { yearly_averages: groupByYear(airport, cells)};
			let html = mustache.render(template,  { yearly_averages: groupByYear(airport, cells)});
		res.send(html);
	})
});
	
app.listen(port);
