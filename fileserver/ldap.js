const ldapjs = require('ldapjs');
const server = ldapjs.createServer();
const config = require('./config.js');

const PORT = process.env.PORT || config.LDAP_PORT;

server.listen(PORT, function() {
  console.log("Listening to PORT:- " + PORT)
})