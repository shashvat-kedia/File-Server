const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const uploader = multer({dest: '/uploads'});
const app = express();

const PORT = 8080 || process.env.PORT;

app.use(express.static(__dirname + "/uploads"))

app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.use(cors())

app.get("/",function(req,res){
  console.log("Hit home");
})

app.post("/text",function(req,res){
  console.log("Hit on home")
  res.json({
    "status": 200,
    "message": "Success"
  })
})

app.post("/upload",uploader.single("file_tag"),function(req,res){
  const tempPath = req.file.path
  const destPath = path.join(__dirname + "/uploads/" + Math.round((new Date()).getTime())/1000 + ".png");
  fc.rename(tempPath,destPath,function(err){
    if(err){
      console.error(err)
      res.status(500).send({
        "message": "Internal server error"
      })
    }
    res.status(200).send({
      "message": "File upload successfull"
    })
  })
})

app.listen(PORT,function(){
  console.log("Listening to port: " + PORT)
})