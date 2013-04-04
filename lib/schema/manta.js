// Copyright (c) 2013 Joyent, Inc.  All rights reserved.



///--- Exports

module.exports = {
    // return either the string up to the last occurence of a /.  in the case
    // there is no slash or only a leading slash, return the entire string.
    manta: function(key) {
        var lastIndex = key.lastIndexOf('/');
        if (lastIndex === 0 || lastIndex === -1) {
            return key;
        } else {
            return key.substr(0, lastIndex);
        }
    }
}
