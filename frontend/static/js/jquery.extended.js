jQuery.fn.dropDownMenu = function(name, duration) {
  return this.each(function(){
      var that = $(this);
      var form = that.find('#options_form');
      form.hide();
      that.append("<div class='button'>"+name+"</div>").find('div.button').toggle(
          function() {
              form.show();
          },
          function() {
              form.hide();
          }
          );
  });
};