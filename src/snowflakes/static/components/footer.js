'use strict';
var React = require('react');
var Modal = require('react-bootstrap/lib/Modal');
var OverlayMixin = require('react-bootstrap/lib/OverlayMixin');

var Footer = React.createClass({
    contextTypes: {
        session: React.PropTypes.object
    },

    propTypes: {
        version: React.PropTypes.string // App version number
    },

    render: function() {
        var session = this.context.session;
        var disabled = !session;
        var userActionRender;

        if (!(session && session['auth.userid'])) {
					 userActionRender = <LoginBoxes/>
        } else {
            userActionRender = <a href="#" data-trigger="logout">Submitter sign out</a>;
        }
        return (
            <footer id="page-footer">
                <div className="container">
                    <div className="row">
                        <div className="app-version">{this.props.version}</div>
                    </div>
                </div>
                <div className="page-footer">
                    <div className="container">
                        <div className="row">
                            <div className="col-sm-6 col-sm-push-6">
                                <ul className="footer-links">
                                    <li><a href="mailto:encode-help@lists.stanford.edu">Contact</a></li>
                                    <li><a href="http://www.stanford.edu/site/terms.html">Terms of Use</a></li>
                                    <li id="user-actions-footer">{userActionRender}</li>
                                </ul>
                                <p className="copy-notice">&copy;{new Date().getFullYear()} Stanford University.</p>
                            </div>

                            <div className="col-sm-6 col-sm-pull-6">
                                <ul className="footer-logos">
                                    <li><a href="/"><img src="/static/img/encode-logo-small-2x.png" alt="ENCODE" id="encode-logo" height="45px" width="78px" /></a></li>
                                    <li><a href="http://www.ucsc.edu"><img src="/static/img/ucsc-logo-white-alt-2x.png" alt="UC Santa Cruz" id="ucsc-logo" width="107px" height="42px" /></a></li>
                                    <li><a href="http://www.stanford.edu"><img src="/static/img/su-logo-white-2x.png" alt="Stanford University" id="su-logo" width="105px" height="49px" /></a></li>
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            </footer>
        );
    }
});

// LoginBox Popup
var LoginBoxes = React.createClass({
  mixins: [OverlayMixin],
  getInitialState: function() {
  	return {username: '', password: '', isOpen: false};
  },
  usernameFill: function(v) {
  	this.setState({username: v});
  },
  passwordFill: function(v) {
  	this.setState({password: v});
  },
  handleToggle: function () {
	  this.setState({
		  isOpen: !this.state.isOpen
	  });
  },
  handleSubmit: function(e){
    e.preventDefault();
    var username = this.state.username.trim();
	var password = this.state.password.trim();
  	if (username === '' || password === '') {
    	return;
    }
    // do something
    this.setState({username: '', password: ''});
    console.log('EXIT THE PAGE NOW');
  },
  render: function () {
	return (
	  <div>
		  <a id="loginbtn" onClick={this.handleToggle}>Log in</a>
	 </div>
 );
 },

	 renderOverlay: function () {
         if (!this.state.isOpen) {
             return <span/>;
         }
         return (
	  <Modal onRequestHide={this.handleToggle} dialogClassName="login-modal">
        <div className="login-box">
        	<h1 className="title">Account Login</h1>
      		<label className="fill-label">Username:</label>
        	<TextBox default="Username" fill={this.usernameFill} tType="text"/>
        	<label className="fill-label">Password:</label>
        	<TextBox default="Password" fill={this.passwordFill} tType="password"/>
        	<ul className="links">
            	<li><button id="popuploginbtn" className="sexy-btn"
	                onClick={this.handleSubmit}><span>Sign in</span></button></li>
				<li><form action='http://google.com'><button id="regbtn" className="sexy-btn">
					<span>Register</span></button></form></li>
				<li><form action='http://google.com'><button id="passbtn" className="sexy-btn">
					<span>Change password</span></button></form></li>
				{/*<li><a href="https://www.google.com/">Register</a></li>
	            <li><a href="https://www.google.com/">New password</a></li>*/}
	            <li><button id="closebtn" className="sexy-btn"
	                onClick={this.handleToggle}><span>Close</span></button></li>
        	</ul>
      	</div>
  	   </Modal>
    );
  },
});

var TextBox = React.createClass({
  getInitialState: function() {
  	return({data: ''});
  },
  handleFill: function(e) {

  	this.setState({data: e.target.value});
    this.props.fill(e.target.value);
  },
  render: function() {
  	return(
    	<div>
    		<input type={this.props.tType} className="text-box"
    		placeholder={this.props.default} onChange={this.handleFill}
    		value={this.state.data} />
    	</div>
  	);
  },
});

module.exports = Footer;
