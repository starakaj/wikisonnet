var React = require('react');
var $ = require('jQuery');

var PoemForm = React.createClass({
  propTypes: {
    onPoem: React.PropTypes.func.isRequired
  },
  getInitialState: function() {
    return {
      subject: ''
    };
  },
  changeSubject: function(ev) {
    console.log(ev.target.value);
    this.setState({
      subject: ev.target.value
    });
  },
  getPoem: function(ev) {
    ev.preventDefault();

    url = 'http://wikisonnet-dev2.elasticbeanstalk.com/api/v1/compose/' + this.state.subject;
    console.log("Get poem for " + this.state.subject);
    $.getJSON(url,function(data) {
      this.props.onPoem(data);
    }.bind(this));
  },
  render: function() {
    return (
      <form onSubmit={this.getPoem}>
        <div>
          <label htmlFor='subject'>Subject</label>
          <div><input type='text' id='subject' value={this.state.subject} onChange={this.changeSubject} placeholder='Subject' /></div>
        </div>
        <div>
          <button type='submit'>Compose Sonnet</button>
        </div>
      </form>
    );
  }
});

var Poem = React.createClass({
  propTypes: {
    lines: React.PropTypes.array
  },
  getInitialState: function() {
    return {
      lines: (this.props.lines || [])
    };
  },
  onPoem: function(poem) {
    this.setState({
      lines: poem["poem"]
    });
  },
  render: function() {
    console.log(this.state.lines);
    var lines = this.state.lines.map(function(eachline) {
      return <Line line={eachline[0]}></Line>;
    });

    return (
      <div>
        <PoemForm onPoem={this.onPoem}></PoemForm>
        {lines}
      </div>
    );
  }
});

var Line = React.createClass({
  getInitialState: function() {
    return {
      line: (this.props.line || "")
    };
  },
  render: function() {

    return (
      <p>
      {this.state.line}
      </p>
    );
  }
});

module.exports = Poem;
