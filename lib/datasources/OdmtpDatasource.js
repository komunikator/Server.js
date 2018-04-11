/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A OdmtpDatasource provides queryable access to a Odmtp endpoint. */

var Datasource = require('./Datasource'),
    N3 = require('n3'),
    LRU = require('lru-cache');

var ENDPOINT_ERROR = 'Error accessing Odmtp endpoint';
var INVALID_TURTLE_RESPONSE = 'The endpoint returned an invalid Turtle response.';

// Creates a new OdmtpDatasource
function OdmtpDatasource(options) {
  // console.log('OdmtpDatasource options ', options);

  if (!(this instanceof OdmtpDatasource))
    return new OdmtpDatasource(options);
  Datasource.call(this, options);
  this._countCache = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });

  // Set endpoint URL and default graph
  options = options || {};
  this._endpoint = this._endpointUrl = (options.endpoint || '').replace(/[\?#][^]*$/, '');
  if (!options.defaultGraph)
    this._endpointUrl += '?query=';
  else
    this._endpointUrl += '?default-graph-uri=' + encodeURIComponent(options.defaultGraph) + '&query=';
}
Datasource.extend(OdmtpDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Writes the results of the query to the given triple stream
OdmtpDatasource.prototype._executeQuery = function (query, destination) {
  // Create the HTTP request
  var odmtpPattern = this._createTriplePattern(query), self = this,
      constructQuery = this._createConstructQuery(odmtpPattern, query.offset, query.limit),
      request = { url: this._endpointUrl + encodeURIComponent(constructQuery),
        headers: { accept: 'text/turtle;q=1.0,application/n-triples;q=0.5,text/n3;q=0.3' },
      };

  console.log('request: ', request);
  // Fetch and parse matching triples
  (new N3.Parser()).parse(this._request(request, emitError), function (error, triple) {
    if (!error) {
      if (triple)
        destination._push(triple);
      else
        destination.close();
    }
    // Virtuoso sometimes sends invalid Turtle, so try N-Triples.
    // We don't just accept N-Triples right away because it is slower,
    // and some Virtuoso versions don't support it and/or get conneg wrong.
    else {
      request.headers.accept = 'application/n-triples';
      return (new N3.Parser()).parse(self._request(request, emitError), function (error, triple) {
        if (error)
          emitError(new Error(INVALID_TURTLE_RESPONSE));
        else if (triple)
          destination._push(triple);
        else
          destination.close();
      });
    }
  });

  // Determine the total number of matching triples
  this._getPatternCount(odmtpPattern, function (error, totalCount) {
    if (error)
      emitError(error);
    else if (typeof totalCount === 'number')
      destination.setProperty('metadata', { totalCount: totalCount, hasExactCount: true });
  });

  // Emits an error on the triple stream
  function emitError(error) {
    error && destination.emit('error', new Error(ENDPOINT_ERROR + ' ' + self._endpoint + ': ' + error.message));
  }
};

// Retrieves the (approximate) number of triples that match the Odmtp pattern
OdmtpDatasource.prototype._getPatternCount = function (odmtpPattern, callback) {
  // Try to find a cache match
  var cache = this._countCache, count = cache.get(odmtpPattern);
  if (count) return setImmediate(callback, null, count);

  // Execute the count query
  var countResponse = this._request({
    url: this._endpointUrl + encodeURIComponent(this._createCountQuery(odmtpPattern)),
    headers: { accept: 'text/csv' },
    timeout: 7500,
  }, callback);

  // Parse Odmtp response in CSV format (2 lines: variable name / count value)
  var csv = '';
  countResponse.on('data', function (data) { csv += data; });
  countResponse.on('end', function () {
    var countMatch = csv.match(/\d+/);
    if (!countMatch)
      callback(new Error('COUNT query failed.'));
    else {
      var count = parseInt(countMatch[0], 10);
      // Cache large values; small ones are calculated fast anyway
      if (count > 100000)
        cache.set(odmtpPattern, count);
      callback(null, count);
    }
  });
};

// Creates a CONSTRUCT query from the given Odmtp pattern
OdmtpDatasource.prototype._createConstructQuery =  function (odmtpPattern, offset, limit) {
  var query = ['CONSTRUCT', odmtpPattern, 'WHERE', odmtpPattern];
  // Even though the Odmtp spec indicates that
  // LIMIT and OFFSET might be meaningless without ORDER BY,
  // this doesn't seem a problem in practice.
  // Furthermore, sorting can be slow. Therefore, don't sort.
  limit  && query.push('LIMIT',  limit);
  offset && query.push('OFFSET', offset);
  return query.join(' ');
};

// Creates a SELECT COUNT(*) query from the given Odmtp pattern
OdmtpDatasource.prototype._createCountQuery = function (odmtpPattern) {
  return 'SELECT (COUNT(*) AS ?c) WHERE ' + odmtpPattern;
};

// Creates a Odmtp pattern for the given triple pattern
OdmtpDatasource.prototype._createTriplePattern = function (triple) {
  var query = ['{'], literalMatch;

  // Add a possible subject IRI
  triple.subject ? query.push('<', triple.subject, '> ') : query.push('?s ');

  // Add a possible predicate IRI
  triple.predicate ? query.push('<', triple.predicate, '> ') : query.push('?p ');

  // Add a possible object IRI or literal
  if (N3.Util.isIRI(triple.object))
    query.push('<', triple.object, '>');
  else if (!(literalMatch = /^"([^]*)"(?:(@[^"]+)|\^\^([^"]+))?$/.exec(triple.object)))
    query.push('?o');
  else {
    if (!/["\\]/.test(literalMatch[1]))
      query.push('"', literalMatch[1], '"');
    else
      query.push('"""', literalMatch[1].replace(/(["\\])/g, '\\$1'), '"""');
    literalMatch[2] ? query.push(literalMatch[2])
                    : literalMatch[3] && query.push('^^<', literalMatch[3], '>');
  }

  return query.push('}'), query.join('');
};

module.exports = OdmtpDatasource;
