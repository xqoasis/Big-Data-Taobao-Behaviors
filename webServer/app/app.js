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

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}
function intToNumber(c) {
	return Number(Buffer.from(c).readInt8());
}
function rowToMap(row) {
	var stats = {}
	if (row === null) {
		console.log("no information")
	}else {
		row.forEach(function (item) {
			if (item['column'].includes('pv') || item['column'].includes('fav')|| item['column'].includes('cart') || item['column'].includes('buy')) {
				stats[item['column']] = counterToNumber(item['$'])
			}else {
				stats[item['column']] = intToNumber(item['$'])
			}

		});
	}
	return stats;
}

// page to search a category name and return each behavior's count
app.use(express.static('public'));
app.get('/catesum.html',function (req, res) {
    const category=req.query['category'];
	const user_id=req.query['user_id'];
	if (category !== "") {
		console.log(category);
		hclient.table('xqoasis_category_behavior_sum').row(category).get(function (err, cells) {
			const sellsinfo = rowToMap(cells);
			console.log(sellsinfo)
			function cate_behavior_info(bahavior) {
				var bahavior_sum = sellsinfo["behaviorSum:" + bahavior];
				if(bahavior_sum === 0)
					return " - ";
				return bahavior_sum;
			}

			var template = filesystem.readFileSync("result.mustache").toString();
			var html_1 = mustache.render(template,  {
				category : req.query['category'],
				pv_sum : cate_behavior_info("pv"),
				fav_sum : cate_behavior_info("fav"),
				cart_sum : cate_behavior_info("cart"),
				buy_sum : cate_behavior_info("buy")
			});
			res.send(html_1);
		});
	}else if (user_id !== "") {
		console.log(user_id);
		hclient.table('xqoasis_user_behavior_count_score').row(user_id).get(function (err, cells) {
			const sellsinfo = rowToMap(cells);
			console.log(sellsinfo)
			function user_behavior_info(info_type) {
				var info_type = sellsinfo["info:" + info_type];
				if(info_type === 0)
					return " - ";
				return info_type;
			}

			var template_2 = filesystem.readFileSync("result2.mustache").toString();
			var html_2 = mustache.render(template_2,  {
				user_id : req.query['user_id'],
				pv_sum : user_behavior_info("pv"),
				fav_sum : user_behavior_info("fav"),
				cart_sum : user_behavior_info("cart"),
				buy_sum : user_behavior_info("buy"),
				r_val: user_behavior_info("R"),
				r_rank: user_behavior_info("R_rank"),
				r_score: user_behavior_info("R_score"),
				f_val: user_behavior_info("F"),
				f_rank: user_behavior_info("F_rank"),
				f_score: user_behavior_info("F_score"),
				score: user_behavior_info("score")
			});
			res.send(html_2);
		})
	}else {
		console.log("input empty")
	}

});


/* Send simulated behavior information to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/newbehavior.html',function (req, res) {
	var user_id = req.query['user_id'];
	var item_id = req.query['item_id'];
	var category_id = req.query['category_id'];
	var pv_val = !!(req.query['pv']);
	var fav_val = !!(req.query['fav']);
	var cart_val = !!(req.query['cart']);
	var buy_val = !!(req.query['buy']);
	var report = {
		user_id: user_id,
		item_id: item_id,
		category_id : category_id,
		pv_val : pv_val,
		fav_val : fav_val,
		cart_val : cart_val,
		buy_val : buy_val
	};

	kafkaProducer.send([{ topic: 'xqoasis_user_behavior', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-behavior.html');
		});
});

app.listen(port);
