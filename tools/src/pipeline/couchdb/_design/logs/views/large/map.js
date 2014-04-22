function(doc) {
  if(doc._attachments.logfile.length > 1000000) {
    emit(doc._id, doc);
  }
}
