from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^geodjango/', include('geodjango.foo.urls')),

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # (r'^admin/', include(admin.site.urls)),
    
    (r'^geo/$','location.views.main'),
    (r'^geo/geodb/(?P<level_code>\w+)/$','location.views.geodb'),
    (r'^geo/fips/$','location.views.fips'),
    (r'^geo/regions/(?P<level_code>\w+)/$','location.views.regions')
    (r'^geo/boundaries/(?P<level_code>\w+)/$','location.views.boundaries')
)
