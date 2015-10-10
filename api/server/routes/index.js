var path = require('path');
var express = require('express');
var router = express.Router();
var pg = require('pg');
var sprintf = require('sprintf');
var connectionString = require(path.join(__dirname, '../', '../', 'config'));

/* GET home page. */
router.get('/', function(req, res, next) {
  res.sendFile(path.join(__dirname, '../', '../', 'client', 'views', 'index.html'));
});

router.get('/api/v1/random', function(req, res) {
    var results = [];
    // Get a Postgres client from the connection pool
    pg.connect(connectionString, function(err, client, done) {
        // SQL Query > Select Data
        var query = client.query("SELECT * FROM iambic_lines OFFSET random() * (SELECT count(*) FROM iambic_lines) LIMIT 1 ;");
        // Stream results back one row at a time
        query.on('row', function(row) {
            results.push(row);
        });
        // After all data is returned, close connection and return results
        query.on('end', function() {
            client.end();
            return res.json(results);
        });
        // Handle Errors
        if(err) {
          console.log(err);
        }
    });
});

router.get('/api/v1/randomrhymer', function(req, res, next) {

    var rhyme_class = req.query["last_stressed_vowel"];
    var word = req.query["word"];

    if (rhyme_class == null || word == null) {
      console.log("No rhyme class or word provided");
      console.log(req.query);
      next();
    }

    else {
      var results = [];
      pg.connect(connectionString, function(err, client, done) {
          var querystr = sprintf("SELECT * FROM iambic_lines WHERE last_stressed_vowel = '%s' "+
                                " AND word != '%s' OFFSET random() * (SELECT count(*) FROM iambic_lines " +
                                "WHERE last_stressed_vowel = '%s' AND word != '%s') LIMIT 1", rhyme_class, word, rhyme_class, word);
          console.log(querystr);
          var query = client.query(querystr);
          query.on('row', function(row) {
              results.push(row);
          });
          query.on('end', function() {
              client.end();
              return res.json(results);
          });
          if(err) {
            console.log(err);
          }
      });
    }
});

module.exports = router;
