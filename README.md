# File-Server
A File Server based on HTTP using NodeJS and AWS S3 which stores files by breaking them into smaller chunks and also provides support for multi threaded downloads. Redis is used for caching chunks that are fetched from AWS S3.

# TO-DO
- [ ] Shift to WebDAV
- [ ] Implement auth service
- [ ] Push notifications for file sync
- [ ] Split into microservices
