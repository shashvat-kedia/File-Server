const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();

const PORT = 8080 || process.env.PORT;

app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.use(cors())

app.get("/",function(req,res){
  console.log("Hit on home")
})

app.listen(PORT,function(){
  console.log("Listening to port: " + PORT)
})