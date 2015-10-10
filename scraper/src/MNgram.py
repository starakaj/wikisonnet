"""Microsoft Web N-Gram functions

This module provides simplified access to the Microsoft Web N-Gram web service.

To get a list of models currently supported:

    MicrosoftNgram.GetModels()

To create a lookup object:

    lookup = MicrosoftNgram.LookupService(token, model, serviceURI)

        [token] can be omitted if you provide an environment variable named
        NGRAM_TOKEN.

        [model] can be omitted if you provide an environment variable named
        NGRAM_MODEL.  If the argument is not provided and neither is the env-
        ironment variable, a default model is used.  This value is currently
        'bing-body/2013-12/3'.  The model can be specified either in the REST 
        format ({stream}/{version}/{order}) or in the URN format (urn:ngram:
        {stream}:{version}:{order}).

        [serviceURI] should be omitted unless the service location has
        changed. You can provide a default override with the enviroment variable
        named NGRAM_SERVICEURI.

        Internally, urllib/urllib2 modules are used to call the web service.
        This means that if you need to make web requests via a proxy, you must
        first set the environment variable HTTP_PROXY to an approriate value.

If you like, you can get/set the model after the lookup object is created:

    lookup.GetModel()

    lookup.SetModel(model)

        As with the LookupService constructor, the [model] value can be a path 
        or a URN.

To get the joint probability of the words in a phrase:

    lookup.GetJointProbability(phrase)

To get the conditional probability of the last word in a given phrase:

    lookup.GetConditionalProbability(phrase)

To find the following word in probability order:

    lookup.Generate(phrase,maxgen)

        This method will return an iterator, from which you can get tuples
        containing the ({word},{probability}) pair.

        [maxgen] is optional; this will cap the iterator return count.


DISCLAIMER: This module is provided "AS IS" without warranty of any kind, either
express or implied, including but not limited to the implied warranties of
merchantability and/or fitness for a particular purpose."""

import os
import sys
import urllib
import urllib.parse
import urllib.request
# import urllib2

def GetModels():
    return LookupService.GetModels()

class LookupService(object):
    def __init__(self, token=None, model=None, serviceUri=None):
        self.token = token
        if (token is None):
            self.token = os.getenv('NGRAM_TOKEN')
        else:
            self.token = token
        if (self.token is None):
            raise ValueError('token must be specified, either as an argument, or as an environment variable named NGRAM_TOKEN')

        if (model is None):
            _model = os.getenv('NGRAM_MODEL')
            if (_model is not None):
                self.model = LookupService._parseModel(_model)
            else:
                self.model = "bing-body/2013-12/3"
        else:
            self.model = LookupService._parseModel(model);

        if (serviceUri is None):
            self.serviceUri = os.getenv('NGRAM_SERVICEURI')
            if (self.serviceUri is None):
                self.serviceUri = "http://weblm.research.microsoft.com/rest.svc/";
        else:
            self.serviceUri = serviceUri

    @staticmethod
    def _parseModel(model):
        import re
        result = re.match('urn:ngram:(.*):(.*):(\d+)', model)
        return result.group(1) + "/" + result.group(2) + "/" + result.group(3) if (result is not None) else model

    @staticmethod
    def GetModels():
        service = LookupService(token='bogus')
        return urlopen(service.serviceUri).read().split('\r\n') # defines a tuple on the fly
    
    def SetModel(self,model):
        self.model = LookupService._parseModel(model)

    def GetModel(self):
        return self.model

    def _getData(self, phrase, operation, args=None):
        if (self.model is None):
            raise ValueError('model must be specified, either as an argument to the LookupService constructor, or as an environment variable named NGRAM_MODEL')
        urlAddr=self.serviceUri+self.model+'/'+operation+'?p='+urllib.parse.quote(phrase)+'&u='+self.token
        if (args is not None):
            for k,v in args.iteritems():
                urlAddr = urlAddr + '&' + k + '=' + urllib.parse.quote(str(v))
        return urllib.request.urlopen(urlAddr).read()

    def _getProbabilityData(self, phrase, operation):
        return float(self._getData(phrase, operation))

    def GetJointProbability(self, phrase):
        return self._getProbabilityData(phrase,'jp')

    def GetConditionalProbability(self, phrase):
        return self._getProbabilityData(phrase,'cp')

    def Generate(self, phrase, maxgen=None):
        nstop = sys.maxint if (maxgen is None) else maxgen;
        arg = {}
        while (True):
            arg['n'] = min(1000, max(0, nstop));
            result = self._getData(phrase, 'gen', arg).split('\r\n')
            if (len(result) <= 2):
                break;
            nstop -= len(result) - 2;
            arg['cookie'] = result[0]
            backoff = result[1]
            for x in result[2:]:
                pair = x.split(';')
                yield pair[0], float(pair[1])
