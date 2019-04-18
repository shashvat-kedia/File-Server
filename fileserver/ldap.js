const ldapjs = require('ldapjs');
const server = ldapjs.createServer();
const config = require('./config.js');

const PORT = process.env.PORT || config.LDAP_PORT;

function authorize(req, res, next) {
  if (!req.connection.ldap.binDN.equals('cn=root')) {
    return next(new ldap.InsufficientAccessRightsError())
  }
  return next()
}

server.listen(PORT, config.LDAP_URL, function() {
  console.log("Listening to PORT:- " + PORT)
})