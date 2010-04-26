# Create your views here.

from django.http import HttpResponse


def main(request):
	return HttpResponse('Hello, world.')
	