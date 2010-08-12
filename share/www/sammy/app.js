
// -- new style templates --
// this.get('#/', function() {
//   this.render('index.mustache')
//       .replace('#main')
//       .render('items.json')
//       .renderEach('item.mustache')
//       .appendTo('#main ul');
// });

var app = {};
window.app = app;

var isEven = function (someNumber) {
    return (someNumber%2 == 0) ? true : false;
};
var formatSize = function (size) {
  var jump = 512;
  if (size < jump) return size + " bytes";
  var units = ["KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
  var i = 0;
  while (size >= jump && i < units.length) {
    i += 1;
    size /= 1024
  }
  return size.toFixed(1) + ' ' + units[i - 1];
}
  
app.index = function () {
  $('h1#topbar').append('<strong>Overview</strong>');
  var dbRow = function (name, even) {
    var row = $('<tr id=db-"'+name+'"><th><a href="#/'+name+'">'+name+'</a></th></tr>');
    
    row.addClass(even ? "even" : "odd")    
    row.appendTo('tbody.content');
    $.ajax({ dataType: 'json', url: '/'+name 
           , success: function (info) {              
               row.append('<td class="size">'+formatSize(info.disk_size)+'</td>' +
                          '<td class="count">'+info.doc_count+'</td>' + 
                          '<td class="seq">'+info.update_seq+'</td>'
                          )
                          ;   
           }
           , error: function (info) {
               row.append('<td class="size">error</td>' +
                          '<td class="count">error</td>' +
                          '<td class="seq">error</td>'
                          )
                          ;
           }
    });
  }
  
  var moreRows = function (dbs, start) {
    for (var i=start;i<(start + 20);i+=1) { 
       if (dbs[i]) dbRow(dbs[i], isEven(i));
       else {$('span.more').remove(); return;}
    }
    $('span.more').unbind('click');
    $('span.more').click(function ( ) { moreRows(dbs, i) })
  }

  $.ajax({ dataType: 'json', url: '/_all_dbs' 
         , success: function (dbs) { 
             if (dbs.length > 20) {
               $('td.more').append('<span class="more">Load 20 More Items</span>');
             }
             moreRows(dbs, 0);
         }
         , error: function () {
           // Add a good error message on the page. 
         }    
  });
}
app.showDatabase = function () {
  var db = this.params['db'];
  
}

var a = $.sammy(function () {
  
  var indexRoute = function () {
    this.render('templates/index.mustache').replace('#content').then(app.index);
  }
  this.get('', indexRoute);
  this.get("#/", indexRoute);
  this.get('#/:db', app.showDatabase)
}).run();

