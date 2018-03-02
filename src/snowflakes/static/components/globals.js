'use strict';
var Registry = require('../libs/registry');
var _ = require('underscore');

// Item pages
module.exports.content_views = new Registry();

// Panel detail views
module.exports.panel_views = new Registry();

// Listing detail views
module.exports.listing_views = new Registry();

// Cell name listing titles
module.exports.listing_titles = new Registry();

// Blocks
module.exports.blocks = new Registry();

// Graph detail view
module.exports.graph_detail = new Registry();

// Document panel components
// +---------------------------------------+
// | header                                |
// +---------------------------+-----------+
// |                           |           |
// |          caption          |  preview  |
// |                           |           |
// +---------------------------+-----------+
// | file                                  |
// +---------------------------------------+
// | detail                                |
// +---------------------------------------+
var document_views = {};
document_views.header = new Registry();
document_views.caption = new Registry();
document_views.preview = new Registry();
document_views.file = new Registry();
document_views.detail = new Registry();
module.exports.document_views = document_views;


var itemClass = module.exports.itemClass = function (context, htmlClass) {
    htmlClass = htmlClass || '';
    (context['@type'] || []).forEach(function (type) {
        htmlClass += ' type-' + type;
    });
    return statusClass(context.status, htmlClass);
};

var statusClass = module.exports.statusClass = function (status, htmlClass) {
    htmlClass = htmlClass || '';
    if (typeof status == 'string') {
        htmlClass += ' status-' + status.toLowerCase().replace(/ /g, '-').replace(/\(|\)/g,'');
    }
    return htmlClass;
};

var validationStatusClass = module.exports.validationStatusClass = function (status, htmlClass) {
    htmlClass = htmlClass || '';
    if (typeof status == 'string') {
        htmlClass += ' validation-status-' + status.toLowerCase().replace(/ /g, '-');
    }
    return htmlClass;
};

module.exports.truncateString = function (str, len) {
    if (str.length > len) {
        str = str.replace(/(^\s)|(\s$)/gi, ''); // Trim leading/trailing white space
        var isOneWord = str.match(/\s/gi) === null; // Detect single-word string
        str = str.substr(0, len - 1); // Truncate to length ignoring word boundary
        str = (!isOneWord ? str.substr(0, str.lastIndexOf(' ')) : str) + '…'; // Back up to word boundary
    }
    return str;
};

// Given an array of objects with @id properties, this returns the same array but with any
// duplicate @id objects removed.
module.exports.uniqueObjectsArray = objects => _(objects).uniq(object =>  object['@id']);

module.exports.bindEvent = function (el, eventName, eventHandler) {
    if (el.addEventListener) {
        // Modern browsers
        el.addEventListener(eventName, eventHandler, false); 
    } else if (el.attachEvent) {
        // IE8 specific
        el.attachEvent('on' + eventName, eventHandler);
    }
};

module.exports.unbindEvent = function (el, eventName, eventHandler) {
    if (el.removeEventListener) {
        // Modern browsers
        el.removeEventListener(eventName, eventHandler, false); 
    } else if (el.detachEvent) {
        // IE8 specific
        el.detachEvent('on' + eventName, eventHandler);
    }
};

module.exports.unreleased_files_url = function (context) {
    var file_states = [
        '',
        "uploading",
        "uploaded",
        "upload failed",
        "format check failed",
        "in progress",
        "released"
    ].map(encodeURIComponent).join('&status=');
    return '/search/?limit=all&type=file&dataset=' + context['@id'] + file_states;
};

// Make the first character of the given string uppercase. Can be less fiddly than CSS text-transform.
// http://stackoverflow.com/questions/1026069/capitalize-the-first-letter-of-string-in-javascript#answer-1026087
String.prototype.uppercaseFirstChar = function(string) {
    return this.charAt(0).toUpperCase() + this.slice(1);
};


// TODO: move this to encode project specific file
module.exports.productionHost = {'www.snovault.org':1, 'snovault.org':1, 'www.snovault.org':1};

var encodeVersionMap = module.exports.encodeVersionMap = {
    "ENCODE2": "2",
    "ENCODE3": "3"
};

// Determine the given object's ENCODE version
module.exports.encodeVersion = function(context) {
    var encodevers = "";
    if (context.award && context.award.rfa) {
        encodevers = encodeVersionMap[context.award.rfa.substring(0,7)];
        if (typeof encodevers === "undefined") {
            encodevers = "";
        }
    }
    return encodevers;
};

module.exports.dbxref_prefix_map = {
    "PMID": "http://www.ncbi.nlm.nih.gov/pubmed/?term=",
    "PMCID": "http://www.ncbi.nlm.nih.gov/pmc/articles/",
    "doi": "https://doi.org/doi:",
    // Antibody RRids
    "AR": "http://antibodyregistry.org/search.php?q="
};
