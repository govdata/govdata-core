define(["jquery-ui"],function(){$.Widget.prototype.listenTo=function(a,b,c){var d=this;a.element.bind((a.widgetEventPrefix+b).toLowerCase(),function(){c.apply(d,this.arguments)})}});