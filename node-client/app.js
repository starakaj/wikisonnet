var React = require('react');
var Poem = require('./views/index.jsx');

var books = JSON.parse(document.getElementById('initial-data').getAttribute('data-json'));
var fucks = function() {console.log("Fuck")}
React.render(<Poem/>, document.getElementById('container'));
