"use strict";
(self["webpackChunk_openwpm_webext_firefox"] = self["webpackChunk_openwpm_webext_firefox"] || []).push([["src_stealth_stealth_ts"],{

/***/ "./src/stealth/error.ts"
/*!******************************!*\
  !*** ./src/stealth/error.ts ***!
  \******************************/
(__unused_webpack_module, exports) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.generateErrorObject = generateErrorObject;
exports.getBeginOfScriptCalls = getBeginOfScriptCalls;
exports.getStackTrace = getStackTrace;
/*
 * Functionality to generate error objects
 */
function generateErrorObject(err, context = undefined) {
    // TODO: Pass context
    context = context !== undefined ? context : window;
    const cleaned = cleanErrorStack(err.stack);
    const stack = splitStack(cleaned);
    const lineInfo = getLineInfo(stack);
    const fileName = getFileName(stack);
    let fakeError;
    try {
        // fake type, message, filename, column and line
        // const propertyName = "stack";
        fakeError = new context.wrappedJSObject[err.name](err.message, fileName);
        fakeError.lineNumber = lineInfo.lineNumber;
        fakeError.columnNumber = lineInfo.columnNumber;
    }
    catch (error) {
        console.log("ERROR creation failed. Error was:" + error);
    }
    return fakeError;
}
/*
 * Trims traces from the stack, which contain the extionsion ID
 */
function cleanErrorStack(stack) {
    const extensionID = browser.runtime.getURL("");
    const lines = typeof stack !== "string" ? stack : splitStack(stack);
    lines.forEach((line) => {
        if (line.includes(extensionID)) {
            stack = stack.replace(line + "\n", "");
        }
    });
    return stack;
}
/*
 * Provides the index the first call outside of the extension
 */
function getBeginOfScriptCalls(stack) {
    const extensionID = browser.runtime.getURL("");
    const lines = typeof stack !== "string" ? stack : splitStack(stack);
    for (let i = 0; i < lines.length; i++) {
        if (!lines[i].includes(extensionID)) {
            return i;
        }
    }
    return -1;
}
/*
 * Get the stack as array
 */
function splitStack(stack) {
    return stack.split("\n").map(function (line) {
        return line.trim();
    });
}
/*
 * Retrieves line and column information of the function
 * calling before the extension was involved
 */
function getLineInfo(stack) {
    const firstLine = stack[0];
    const matches = [...firstLine.matchAll(":")];
    const column = firstLine.slice(matches[matches.length - 1].index + 1, firstLine.length);
    const line = firstLine.slice(matches[matches.length - 2].index + 1, matches[matches.length - 1].index);
    return {
        lineNumber: line,
        columnNumber: column,
    };
}
/*
 * Retrieves file name of the function
 * that called before the extension got involved
 */
function getFileName(stack) {
    const firstLine = stack[0];
    const matches_at = [...firstLine.matchAll("@")];
    const matches_colon = [...firstLine.matchAll(":")];
    return firstLine.slice(matches_at[matches_at.length - 1].index + 1, matches_colon[matches_colon.length - 2].index);
}
// function getOriginFromStackTrace(err, includeStack){
//   console.log(err.stack);
//   const stack = splitStack(err.stack);
//   const lineInfo = getLineInfo(stack);
//   const fileName = getFileName(stack);
//   const callSite = stack[1];
//   const callSiteParts = callSite.split("@");
//   const funcName = callSiteParts[0] || "";
//   const items = rsplit(callSiteParts[1], ":", 2);
//   const scriptFileName = items[items.length - 3] || "";
//   const callContext = {
//     scriptUrl,
//     scriptLine: lineInfo.lineNumber,
//     scriptCol: lineInfo.columnNumber,
//     funcName,
//     scriptLocEval,
//     callStack: includeStack ? trace.slice(3).join("\n").trim() : "",
//   };
// }
// Helper to get originating script urls
// Legacy code
function getStackTrace() {
    let stack;
    try {
        throw new Error();
    }
    catch (err) {
        stack = err.stack;
    }
    return stack;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXJyb3IuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvc3RlYWx0aC9lcnJvci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOztBQXdJUyxrREFBbUI7QUFBRSxzREFBcUI7QUFBRSxzQ0FBYTtBQXhJbEU7O0dBRUc7QUFDSCxTQUFTLG1CQUFtQixDQUMxQixHQUF3RCxFQUN4RCxPQUFPLEdBQUcsU0FBUztJQUVuQixxQkFBcUI7SUFDckIsT0FBTyxHQUFHLE9BQU8sS0FBSyxTQUFTLENBQUMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDO0lBQ25ELE1BQU0sT0FBTyxHQUFHLGVBQWUsQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDM0MsTUFBTSxLQUFLLEdBQUcsVUFBVSxDQUFDLE9BQU8sQ0FBQyxDQUFDO0lBQ2xDLE1BQU0sUUFBUSxHQUFHLFdBQVcsQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUNwQyxNQUFNLFFBQVEsR0FBRyxXQUFXLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDcEMsSUFBSSxTQUFpRCxDQUFDO0lBQ3RELElBQUksQ0FBQztRQUNILGdEQUFnRDtRQUNoRCxnQ0FBZ0M7UUFDaEMsU0FBUyxHQUFHLElBQUksT0FBTyxDQUFDLGVBQWUsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUMsR0FBRyxDQUFDLE9BQU8sRUFBRSxRQUFRLENBQUMsQ0FBQztRQUN6RSxTQUFTLENBQUMsVUFBVSxHQUFHLFFBQVEsQ0FBQyxVQUFVLENBQUM7UUFDM0MsU0FBUyxDQUFDLFlBQVksR0FBRyxRQUFRLENBQUMsWUFBWSxDQUFDO0lBQ2pELENBQUM7SUFBQyxPQUFPLEtBQUssRUFBRSxDQUFDO1FBQ2YsT0FBTyxDQUFDLEdBQUcsQ0FBQyxtQ0FBbUMsR0FBRyxLQUFLLENBQUMsQ0FBQztJQUMzRCxDQUFDO0lBQ0QsT0FBTyxTQUFTLENBQUM7QUFDbkIsQ0FBQztBQUVEOztHQUVHO0FBQ0gs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

/***/ },

/***/ "./src/stealth/instrument.ts"
/*!***********************************!*\
  !*** ./src/stealth/instrument.ts ***!
  \***********************************/
(__unused_webpack_module, exports, __webpack_require__) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.startInstrument = startInstrument;
exports.exportCustomFunction = exportCustomFunction;
const settings_1 = __webpack_require__(/*! ./settings */ "./src/stealth/settings.ts");
const error_1 = __webpack_require__(/*! ./error */ "./src/stealth/error.ts");
/** ************************************
 * OpenWPM legacy code
 ***************************************/
// Counter to cap # of calls logged for each script/api combination
const maxLogCount = 500;
// logCounter
const logCounter = {};
// Prevent logging of gets arising from logging
let inLog = false;
// To keep track of the original order of events
let ordinal = 0;
// Options for JSOperation
const JSOperation = {
    call: "call",
    get: "get",
    get_failed: "get(failed)",
    get_function: "get(function)",
    set: "set",
    set_failed: "set(failed)",
    set_prevented: "set(prevented)",
};
// from http://stackoverflow.com/a/5202185
function rsplit(source, sep, maxsplit) {
    const split = source.split(sep);
    return maxsplit
        ? [split.slice(0, -maxsplit).join(sep)].concat(split.slice(-maxsplit))
        : split;
}
// Helper for JSONifying objects
function serializeObject(object, 
// stringifyFunctions: boolean = false,
stringifyFunctions) {
    // Handle permissions errors
    try {
        if (object === null) {
            return "null";
        }
        if (typeof object === "function") {
            return stringifyFunctions ? object.toString() : "FUNCTION";
        }
        if (typeof object !== "object") {
            return object;
        }
        const seenObjects = [];
        return JSON.stringify(object, function (key, value) {
            if (value === null) {
                return "null";
            }
            if (typeof value === "function") {
                return stringifyFunctions ? value.toString() : "FUNCTION";
            }
            if (typeof value === "object") {
                // Remove wrapping on content objects
                if ("wrappedJSObject" in value) {
                    value = value.wrappedJSObject;
                }
                // Serialize DOM elements
                if (value instanceof HTMLElement) {
                    return getPathToDomElement(value);
                }
                // Prevent serialization cycles
                if (key === "" || seenObjects.indexOf(value) < 0) {
                    seenObjects.push(value);
                    return value;
                }
                else {
                    return typeof value;
                }
            }
            return value;
        });
    }
    catch (error) {
        console.log("OpenWPM: SERIALIZATION ERROR: " + error);
        return "SERIALIZATION ERROR: " + error;
    }
}
// Rough implementations of Object.getPropertyDescriptor and Object.getPropertyNames
// See http://wiki.ecmascript.org/doku.php?id=harmony:extended_object_api
Object.getPropertyDescriptor = function (subject, name) {
    if (subject === undefined) {
        throw new Error("Can't get property descriptor for undefined");
    }
    let pd = Object.getOwnPropertyDescriptor(subject, name);
    let proto = Object.getPrototypeOf(subject);
    while (pd === undefined && proto !== null) {
        pd = Object.getOwnPropertyDescriptor(proto, name);
        proto = Object.getPrototypeOf(proto);
    }
    return pd;
};
function updateCounterAndCheckIfOver(scriptUrl, symbol) {
    const key = scriptUrl + "|" + symbol;
    if (key in logCounter && logCounter[key] >= maxLogCount) {
        return true;
    }
    else if (!(key in logCounter)) {
        logCounter[key] = 1;
    }
    else {
        logCounter[key] += 1;
    }
    return false;
}
// Recursively generates a path for an element
function getPathToDomElement(element, visibilityAttr = false) {
    if (element === document.body) {
        return element.tagName;
    }
    if (element.parentNode === null) {
        return "NULL/" + element.tagName;
    }
    let siblingIndex = 1;
    const siblings = element.parentNode.childNodes;
    for (const sibling of siblings) {
        if (sibling === element) {
            let path = getPathToDomElement(element.parentNode, visibilityAttr);
            path += "/" + element.tagName + "[" + siblingIndex;
            path += "," + element.id;
            path += "," + element.className;
            if (visibilityAttr) {
                path += "," + element.hidden;
                path += "," + element.style.display;
                path += "," + element.style.visibility;
            }
            if (element.tagName === "A") {
                path += "," + element.href;
            }
            path += "]";
            return path;
        }
        if (sibling.nodeType === 1 && sibling.tagName === element.tagName) {
            siblingIndex++;
        }
    }
}
function getOriginatingScriptContext(getCallStack = false, isCall = false) {
    const trace = (0, error_1.getStackTrace)().trim().split("\n");
    // return a context object even if there is an error
    const empty_context = {
        scriptUrl: "",
        scriptLine: "",
        scriptCol: "",
        funcName: "",
        scriptLocEval: "",
        callStack: "",
    };
    if (trace.length < 4) {
        return empty_context;
    }
    let traceStart = (0, error_1.getBeginOfScriptCalls)(trace);
    if (traceStart === -1) {
        // If not included, use heuristic, 0-3 or 0-2 are OpenWPMs functions
        traceStart = isCall ? 3 : 4;
    }
    const callSite = trace[traceStart];
    if (!callSite) {
        return empty_context;
    }
    /*
     * Stack frame format is simply: FUNC_NAME@FILENAME:LINE_NO:COLUMN_NO
     *
     * If eval or Function is involved we have an additional part after the FILENAME, e.g.:
     * FUNC_NAME@FILENAME line 123 > eval line 1 > eval:LINE_NO:COLUMN_NO
     * or FUNC_NAME@FILENAME line 234 > Function:LINE_NO:COLUMN_NO
     *
     * We store the part between the FILENAME and the LINE_NO in scriptLocEval
     */
    try {
        let scriptUrl = "";
        let scriptLocEval = ""; // for eval or Function calls
        const callSiteParts = callSite.split("@");
        const funcName = callSiteParts[0] || "";
        const items = rsplit(callSiteParts[1], ":", 2);
        const columnNo = items[items.length - 1];
        const lineNo = items[items.length - 2];
        const scriptFileName = items[items.length - 3] || "";
        const lineNoIdx = scriptFileName.indexOf(" line "); // line in the URL means eval or Function
        if (lineNoIdx === -1) {
            scriptUrl = scriptFileName; // TODO: sometimes we have filename only, e.g. XX.js
        }
        else {
            scriptUrl = scriptFileName.slice(0, lineNoIdx);
            scriptLocEval = scriptFileName.slice(lineNoIdx + 1, scriptFileName.length);
        }
        const callContext = {
            scriptUrl,
            scriptLine: lineNo,
            scriptCol: columnNo,
            funcName,
            scriptLocEval,
            callStack: getCallStack ? trace.slice(3).join("\n").trim() : "",
        };
        return callContext;
    }
    catch (e) {
        console.log("OpenWPM: Error parsing the script context", e.toString(), callSite);
        return empty_context;
    }
}
// function logErrorToConsole(error, context = false) {
//     console.error("OpenWPM: Error name: " + error.name);
//     console.error("OpenWPM: Error message: " + error.message);
//     console.error("OpenWPM: Error filename: " + error.fileName);
//     console.error("OpenWPM: Error line number: " + error.lineNumber);
//     console.error("OpenWPM: Error stack: " + error.stack);
//     if (context) {
//         console.error("OpenWPM: Error context: " + JSON.stringify(context));
//     }
// }
// For gets, sets, etc. on a single value
function logValue(instrumentedVariableName, // : string,
value, // : any,
operation, // : string, // from JSOperation object please
callContext, // : any,
logSettings = {
    depth: 0,
    excludedProperties: [],
    logCallStack: false,
    logFunctionGets: false,
    nonExistingPropertiesToInstrument: [],
    preventSets: false,
    propertiesToInstrument: [],
    recursive: false,
    logFunctionsAsStrings: false,
}) {
    if (inLog) {
        return;
    }
    inLog = true;
    const overLimit = updateCounterAndCheckIfOver(callContext.scriptUrl, instrumentedVariableName);
    if (overLimit) {
        inLog = false;
        return;
    }
    const msg = {
        operation,
        symbol: instrumentedVariableName,
        value: serializeObject(value, logSettings.logFunctionsAsStrings),
        scriptUrl: callContext.scriptUrl,
        scriptLine: callContext.scriptLine,
        scriptCol: callContext.scriptCol,
        funcName: callContext.funcName,
        scriptLocEval: callContext.scriptLocEval,
        callStack: callContext.callStack,
        ordinal: ordinal++,
    };
    try {
        notify("logValue", msg);
    }
    catch (error) {
        console.log("OpenWPM: Unsuccessful value log!");
        // Activate for debugging purpose
        // logErrorToConsole(error);
    }
    inLog = false;
}
// For functions
function logCall(instrumentedFunctionName, args, callContext) {
    if (inLog) {
        return;
    }
    inLog = true;
    const overLimit = updateCounterAndCheckIfOver(callContext.scriptUrl, instrumentedFunctionName);
    if (overLimit) {
        inLog = false;
        return;
    }
    try {
        // Convert special arguments array to a standard array for JSONifying
        const serialArgs = [];
        for (const arg of args) {
            serialArgs.push(serializeObject(arg, false)); // TODO: Get back to logSettings.logFunctionsAsStrings));
        }
        const msg = {
            operation: JSOperation.call,
            symbol: instrumentedFunctionName,
            args: serialArgs,
            value: "",
            scriptUrl: callContext.scriptUrl,
            scriptLine: callContext.scriptLine,
            scriptCol: callContext.scriptCol,
            funcName: callContext.funcName,
            scriptLocEval: callContext.scriptLocEval,
            callStack: callContext.callStack,
            ordinal: ordinal++,
        };
        notify("logCall", msg);
    }
    catch (error) {
        console.log("OpenWPM: Unsuccessful call log: " + instrumentedFunctionName);
        // Activate for debugging purpose
        // console.log(error);
        // logErrorToConsole(error);
    }
    inLog = false;
}
Object.prototype.getPrototypeByDepth = function (subject, depth) {
    if (subject === undefined) {
        throw new Error("Can't get property names for undefined");
    }
    if (depth === undefined || typeof depth !== "number") {
        throw new Error("Depth " + depth + " is invalid");
    }
    let proto = subject;
    for (let i = 1; i <= depth; i++) {
        proto = Object.getPrototypeOf(proto);
    }
    if (proto === undefined) {
        throw new Error("Prototype was undefined. Too deep iteration?");
    }
    return proto;
};
/**
 * Traverses the prototype chain to collect properties. Returns an array containing
 * an object with the depth, propertyNames and scanned subject
 */
Object.prototype.getPropertyNamesPerDepth = function (subject, maxDepth = 0) {
    if (subject === undefined) {
        throw new Error("Can't get property names for undefined");
    }
    const res = [];
    let depth = 0;
    let properties = Object.getOwnPropertyNames(subject);
    res.push({ depth, propertyNames: properties, object: subject });
    let proto = Object.getPrototypeOf(subject);
    while (proto !== null && depth < maxDepth) {
        depth++;
        properties = Object.getOwnPropertyNames(proto);
        res.push({ depth, propertyNames: properties, object: proto });
        proto = Object.getPrototypeOf(proto);
    }
    return res;
};
/**
 * Finds a property along the prototype chain
 */
Object.prototype.findPropertyInChain = function (subject, propertyName) {
    if (subject === undefined || propertyName === undefined) {
        throw new Error("Object and property name must be defined");
    }
    let properties = [];
    let depth = 0;
    while (subject !== null) {
        properties = Object.getOwnPropertyNames(subject);
        if (properties.includes(propertyName)) {
            return { depth, propertyName };
        }
        depth++;
        subject = Object.getPrototypeOf(subject);
    }
    throw Error("Property not found. Check whether configuration is correct!");
};
/*
 * Get all keys for properties that shall be overwritten
 */
function getPropertyKeysToOverwrite(item) {
    const res = [];
    item.logSettings.overwrittenProperties.forEach((obj) => {
        res.push(obj.key);
    });
    return res;
}
function getContextualPrototypeFromString(context, objectAsString) {
    const obj = context[objectAsString];
    if (obj) {
        return obj.prototype ? obj.prototype : Object.getPrototypeOf(obj);
    }
    else {
        return undefined;
    }
}
/**
 * Prepares a list of properties that need to be instrumented
 * Here, this can be a previous created list (settings.js: propertiesToInstrument)
 * or all properties of a given object (settings.js: propertiesToInstrument is empty)
 */
function getObjectProperties(context, item) {
    let propertiesToInstrument = item.logSettings.propertiesToInstrument;
    const proto = getContextualPrototypeFromString(context, item.object);
    if (!proto) {
        throw Error("Object " + item.object + " was undefined.");
    }
    if (propertiesToInstrument === undefined || !propertiesToInstrument.length) {
        propertiesToInstrument = Object.getPropertyNamesPerDepth(proto, item.depth);
        // filter excluded and overwritten properties
        const excluded = getPropertyKeysToOverwrite(item).concat(item.logSettings.excludedProperties);
        propertiesToInstrument = filterPropertiesPerDepth(propertiesToInstrument, excluded);
    }
    else {
        // include the object to each item
        propertiesToInstrument.forEach((propertyList) => {
            propertyList.object = Object.getPrototypeByDepth(proto, propertyList.depth);
        });
    }
    return propertiesToInstrument;
}
/*
 * Enables communication with a background script
 * Must be injected in a private scope to the
 * page context!
 *
 * @param details: property access details
 */
function notify(type, content) {
    content.timeStamp = new Date().toISOString();
    browser.runtime.sendMessage({
        namespace: "javascript-instrumentation",
        type,
        data: content,
    });
}
function filterPropertiesPerDepth(collection, excluded) {
    for (const elem of collection) {
        elem.propertyNames = elem.propertyNames.filter((p) => !excluded.includes(p));
    }
    return collection;
}
/*
 * Injects a function into the page context
 *
 * @param func: Function that shall be exported
 * @param context: target DOM
 * @param name: Name of the function (e.g., get width)
 */
function exportCustomFunction(func, context, name) {
    const targetObject = context.wrappedJSObject.Object.create(null);
    const exportedTry = exportFunction(func, targetObject, {
        allowCrossOriginArguments: true,
        defineAs: name,
    });
    return exportedTry;
}
/*
 * TODO: Add description
 */
function injectFunction(instrumentedFunction, descriptor, functionType, pageObject, propertyName) {
    const exportedFunction = exportCustomFunction(instrumentedFunction, window, propertyName);
    changeProperty(descriptor, pageObject, propertyName, functionType, exportedFunction);
}
/*
 * Add notifications when a property is requested
 * TODO: Bring everything together at this point
 *
 * @param original: the original getter/setter function
 * @param object:
 * @param args:
 */
function instrumentGetObjectProperty(identifier, original, newValue, object, args) {
    const originalValue = original.call(object, ...args);
    const callContext = getOriginatingScriptContext(true);
    const returnValue = newValue !== undefined ? newValue : originalValue;
    logValue(identifier, returnValue, JSOperation.get, callContext);
    return returnValue;
}
/*
 * Add notifications when a property is set
 *
 * @param original: the original getter/setter function
 * @param object:
 * @param args:
 */
function instrumentSetObjectProperty(identifier, original, newValue, object, _args) {
    const callContext = getOriginatingScriptContext(true);
    logValue(identifier, newValue, original ? JSOperation.set : JSOperation.set_failed, callContext);
    return !original ? newValue : original.call(object, newValue);
}
/*
 * Creates a getter function
 *
 * @param descriptor: the descriptor of the original function
 * @param funcName: Name of property/function that shall be overwritten
 * @param newValue: in Case the value shall be changed
 */
function generateGetter(identifier, descriptor, propertyName, newValue = undefined) {
    const original = descriptor.get;
    return Object.getOwnPropertyDescriptor({
        get [propertyName]() {
            return instrumentGetObjectProperty(identifier, original, newValue, this, arguments);
        },
    }, propertyName).get;
}
/*
 * Creates a setter function
 *
 * @param descriptor: the descriptor of the original function
 * @param funcName: Name of property/function that shall be overwritten
 * @param newValue: in Case the value shall be changed
 */
function generateSetter(identifier, descriptor, propertyName, _newValue = undefined) {
    const original = descriptor.set;
    return Object.getOwnPropertyDescriptor({
        set(obj, _prop, value) {
            // _prop === propertyName
            return instrumentSetObjectProperty(identifier, original, value, obj, arguments);
        },
    }, propertyName).set;
}
/*
 * Overwrites the prototype to access a property
 * @param
 */
function changeProperty(descriptor, pageObject, name, method, changed) {
    descriptor[method] = changed;
    Object.defineProperty(pageObject, name, descriptor);
}
/*
 * Retrieves an object in a context
 *
 * @param context: the window object that is currently instrumented
 * @param object: the subobject needed
 */
function getPageObjectInContext(context, context_object) {
    if (context === undefined || context_object === undefined) {
        return;
    }
    return context[context_object].prototype || context[context_object];
}
/*
 * Entry point to creates (g/s)etter functions,
 * instrument them and inject them to the page
 * context
 */
function instrumentGetterSetter(descriptor, identifier, pageObject, propertyName, newValue = undefined) {
    let instrumentedFunction;
    const getFuncType = "get";
    const setFuncType = "set";
    if (Object.prototype.hasOwnProperty.call(descriptor, getFuncType)) {
        instrumentedFunction = generateGetter(identifier, descriptor, propertyName, newValue);
        injectFunction(instrumentedFunction, descriptor, getFuncType, pageObject, propertyName);
    }
    if (Object.prototype.hasOwnProperty.call(descriptor, setFuncType)) {
        instrumentedFunction = generateSetter(identifier, descriptor, propertyName);
        injectFunction(instrumentedFunction, descriptor, setFuncType, pageObject, propertyName);
    }
}
/*
 * TODO: Add description
 */
function functionGenerator(_context, identifier, original, _funcName) {
    function temp() {
        let result;
        const callContext = getOriginatingScriptContext(true, true);
        logCall(identifier, arguments, callContext);
        try {
            result =
                arguments.length > 0
                    ? original.call(this, ...arguments)
                    : original.call(this);
        }
        catch (err) {
            const fakeError = (0, error_1.generateErrorObject)(err);
            throw fakeError;
        }
        return result;
    }
    return temp;
}
/*
 * TODO: Add description
 */
function instrumentFunction(context, descriptor, identifier, pageObject, propertyName) {
    const original = descriptor.value;
    const tempFunction = functionGenerator(context, identifier, original, propertyName);
    const exportedFunction = exportCustomFunction(tempFunction, context, original.name);
    changeProperty(descriptor, pageObject, propertyName, "value", exportedFunction);
}
/*
 * Helper class to perform all needed functionality
 *
 * @param context: the window object that is currently instrumented
 * @param object: child object that shall be instumented
 */
function instrument(context, item, depth, propertyName, newValue = undefined) {
    try {
        const identifier = item.instrumentedName + "." + propertyName;
        const initialPageObject = getPageObjectInContext(context.wrappedJSObject, item.object);
        const pageObject = Object.getPrototypeByDepth(initialPageObject, depth);
        const descriptor = Object.getPropertyDescriptor(pageObject, propertyName);
        if (descriptor === undefined) {
            // Do not do undefined descriptor. We can safely skip them
            return;
        }
        if (typeof descriptor.value === "function") {
            instrumentFunction(context, descriptor, identifier, pageObject, propertyName);
        }
        else {
            instrumentGetterSetter(descriptor, identifier, pageObject, propertyName, newValue);
        }
    }
    catch (error) {
        console.error(error);
        console.error(error.stack);
        return;
    }
}
/*
 * Checks if an object was already wrapped
 * Unwrapped objects should be wrapped immediately
 */
const wrappedObjects = [];
function needsWrapper(object) {
    if (wrappedObjects.some((obj) => object === obj)) {
        return false;
    }
    wrappedObjects.push(object);
    return true;
}
function startInstrument(context) {
    for (const item of settings_1.jsInstrumentationSettings) {
        // retrieve Object properties alont the chain
        let propertyCollection;
        try {
            propertyCollection = getObjectProperties(context, item);
        }
        catch (err) {
            console.error(err);
            continue;
        }
        // Instrument each Property per object/prototype
        if (propertyCollection[0] !== "") {
            propertyCollection.forEach(({ depth, propertyNames, object }) => {
                if (needsWrapper(object)) {
                    propertyNames.forEach((propertyName) => instrument(context, item, depth, propertyName));
                }
            });
        }
        // Instrument properties and overwrite their return value
        if (item.logSettings.overwrittenProperties) {
            item.logSettings.overwrittenProperties.forEach(({ key: name, value }) => {
                const proto = getContextualPrototypeFromString(context, item.object);
                if (proto) {
                    const { depth, propertyName } = Object.findPropertyInChain(proto, name);
                    instrument(context, item, depth, propertyName, value);
                }
                else {
                    console.error("Could not instrument " +
                        item.object +
                        ". Encountered undefined object.");
                }
            });
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5zdHJ1bWVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9zdGVhbHRoL2luc3RydW1lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUE4MUJTLDBDQUFlO0FBQUUsb0RBQW9CO0FBOTFCOUMseUNBQXVEO0FBQ3ZELG1DQUlpQjtBQUdqQjs7eUNBRXlDO0FBQ3pDLG1FQUFtRTtBQUNuRSxNQUFNLFdBQVcsR0FBRyxHQUFHLENBQUM7QUFDeEIsYUFBYTtBQUNiLE1BQU0sVUFBVSxHQUFHLEVBQUUsQ0FBQztBQUN0QiwrQ0FBK0M7QUFDL0MsSUFBSSxLQUFLLEdBQVksS0FBSyxDQUFDO0FBQzNCLGdEQUFnRDtBQUNoRCxJQUFJLE9BQU8sR0FBRyxDQUFDLENBQUM7QUFFaEIsMEJBQTBCO0FBQzFCLE1BQU0sV0FBVyxHQUFHO0lBQ2xCLElBQUksRUFBRSxNQUFNO0lBQ1osR0FBRyxFQUFFLEtBQUs7SUFDVixVQUFVLEVBQUUsYUFBYTtJQUN6QixZQUFZLEVBQUUsZUFBZTtJQUM3QixHQUFHLEVBQUUsS0FBSztJQUNWLFVBQVUsRUFBRSxhQUFhO0lBQ3pCLGFBQWEsRUFBRSxnQkFBZ0I7Q0FDaEMsQ0FBQztBQUVGLDBDQUEwQztBQUMxQyxTQUFTLE1BQU0sQ0FBQyxNQUFjLEVBQUUsR0FBVyxFQUFFLFFBQWdCO0lBQzNELE1BQU0sS0FBSyxHQUFHLE1BQU0sQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUM7SUFDaEMsT0FBTyxRQUFRO1FBQ2IsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUUsQ0FBQyxRQUFRLENBQUMsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBQ3RFLENBQUMsQ0FBQyxLQUFLLENBQUM7QUFDWixDQUFDO0FBRUQsZ0NBQWdDO0FBQ2hDLFNBQVMsZUFB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

/***/ },

/***/ "./src/stealth/settings.ts"
/*!*********************************!*\
  !*** ./src/stealth/settings.ts ***!
  \*********************************/
(__unused_webpack_module, exports) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.jsInstrumentationSettings = void 0;
exports.jsInstrumentationSettings = [
    {
        object: "ScriptProcessorNode", // Depcrecated. Replaced by AudioWorkletNode
        instrumentedName: "ScriptProcessorNode",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "AudioWorkletNode",
        instrumentedName: "AudioWorkletNode",
        depth: 1,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "GainNode",
        instrumentedName: "GainNode",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "AnalyserNode",
        instrumentedName: "AnalyserNode",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "OscillatorNode",
        instrumentedName: "OscillatorNode",
        depth: 1,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    // Add shared prototype by AnalyserNode, OscillatorNode, ScriptProcessorNode, GainNode, ScriptProcessorNode
    {
        object: "AnalyserNode",
        instrumentedName: "Node",
        depth: 1,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "OfflineAudioContext",
        instrumentedName: "OfflineAudioContext",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "AudioContext",
        instrumentedName: "AudioContext",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    // Add shared prototype by AudioContenxt/OfflineAudioContext
    {
        object: "AudioContext",
        instrumentedName: "[AudioContenxt|OfflineAudioContext]",
        depth: 1,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "RTCPeerConnection",
        instrumentedName: "RTCPeerConnection",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "HTMLCanvasElement",
        instrumentedName: "HTMLCanvasElement",
        depth: 1,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: ["style", "offsetWidth", "offsetHeight"],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "Storage",
        instrumentedName: "Storage",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "Navigator",
        instrumentedName: "Navigator",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [{ key: "webdriver", value: false, level: 0 }],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "CanvasRenderingContext2D",
        instrumentedName: "CanvasRenderingContext2D",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [
                "transform",
                "globalAlpha",
                "clearRect",
                "closePath",
                "canvas",
                "quadraticCurveTo",
                "lineTo",
                "moveTo",
                "setTransform",
                "drawImage",
                "beginPath",
                "translate",
            ],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "Screen",
        instrumentedName: "Screen",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [],
            // in OpenWPM is only this one used:
            // {"depth":0, "propertyNames":["colorDepth","pixelDepth"
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: false,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
    {
        object: "document",
        instrumentedName: "document",
        depth: 0,
        logSettings: {
            propertiesToInstrument: [{ depth: 2, propertyNames: ["referrer"] }],
            nonExistingPropertiesToInstrument: [],
            excludedProperties: [],
            overwrittenProperties: [],
            logCallStack: true,
            logFunctionsAsStrings: false,
            logFunctionGets: false,
            preventSets: false,
            recursive: false,
            depth: 5,
        },
    },
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2V0dGluZ3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi9zcmMvc3RlYWx0aC9zZXR0aW5ncy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFFYSxRQUFBLHlCQUF5QixHQUF5QjtJQUM3RDtRQUNFLE1BQU0sRUFBRSxxQkFBcUIsRUFBRSw0Q0FBNEM7UUFDM0UsZ0JBQWdCLEVBQUUscUJBQXFCO1FBQ3ZDLEtBQUssRUFBRSxDQUFDO1FBQ1IsV0FBVyxFQUFFO1lBQ1gsc0JBQXNCLEVBQUUsRUFBRTtZQUMxQixpQ0FBaUMsRUFBRSxFQUFFO1lBQ3JDLGtCQUFrQixFQUFFLEVBQUU7WUFDdEIscUJBQXFCLEVBQUUsRUFBRTtZQUN6QixZQUFZLEVBQUUsS0FBSztZQUNuQixxQkFBcUIsRUFBRSxLQUFLO1lBQzVCLGVBQWUsRUFBRSxLQUFLO1lBQ3RCLFdBQVcsRUFBRSxLQUFLO1lBQ2xCLFNBQVMsRUFBRSxLQUFLO1lBQ2hCLEtBQUssRUFBRSxDQUFDO1NBQ1Q7S0FDRjtJQUVEO1FBQ0UsTUFBTSxFQUFFLGtCQUFrQjtRQUMxQixnQkFBZ0IsRUFBRSxrQkFBa0I7UUFDcEMsS0FBSyxFQUFFLENBQUM7UUFDUixXQUFXLEVBQUU7WUFDWCxzQkFBc0IsRUFBRSxFQUFFO1lBQzFCLGlDQUFpQyxFQUFFLEVBQUU7WUFDckMsa0JBQWtCLEVBQUUsRUFBRTtZQUN0QixxQkFBcUIsRUFBRSxFQUFFO1lBQ3pCLFlBQVksRUFBRSxLQUFLO1lBQ25CLHFCQUFxQixFQUFFLEtBQUs7WUFDNUIsZUFBZSxFQUFFLEtBQUs7WUFDdEIsV0FBVyxFQUFFLEtBQUs7WUFDbEIsU0FBUyxFQUFFLEtBQUs7WUFDaEIsS0FBSyxFQUFFLENBQUM7U0FDVDtLQUNGO0lBRUQ7UUFDRSxNQUFNLEVBQUUsVUFBVTtRQUNsQixnQkFBZ0IsRUFBRSxVQUFVO1FBQzVCLEtBQUssRUFBRSxDQUFDO1FBQ1IsV0FBVyxFQUFFO1lBQ1gsc0JBQXNCLEVBQUUsRUFBRTtZ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

/***/ },

/***/ "./src/stealth/stealth.ts"
/*!********************************!*\
  !*** ./src/stealth/stealth.ts ***!
  \********************************/
(__unused_webpack_module, exports, __webpack_require__) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
/* Taken from https://github.com/kkapsner/CanvasBlocker with small changes
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
const instrument_1 = __webpack_require__(/*! ./instrument */ "./src/stealth/instrument.ts");
// Declaring some local trackers
const interceptedWindows = new WeakMap();
const proxies = new Map();
const changedToStrings = new WeakMap();
// Entry point for this extension
(function () {
    // console.log("Starting frame script");
    try {
        interceptWindow(window);
    }
    catch (error) {
        console.log("Instrumentation initialisation crashed. Reason: " + error);
        console.log(error.stack);
    }
    // console.log("Starting frame script");
})();
function interceptWindow(context) {
    let wrappedTry;
    try {
        wrappedTry = getWrapped(context);
    }
    catch (error) {
        // we are unable to read the location due to SOP
        // therefore we also can not intercept anything.
        // console.log("NOT intercepting window due to SOP: ", context);
        return false;
    }
    const wrappedWindow = wrappedTry;
    if (interceptedWindows.get(wrappedWindow)) {
        // console.log("Already intercepted: ", context);
        return false;
    }
    // console.log("intercepting window", context);
    (0, instrument_1.startInstrument)(context);
    interceptedWindows.set(wrappedWindow, true);
    // console.log("prepare to intercept "+ context.length +" (i)frames.");
    function interceptAllFrames() {
        const currentLength = context.length;
        for (let i = currentLength; i--;) {
            if (!interceptedWindows.get(wrappedWindow[i])) {
                interceptWindow(context[i]);
            }
        }
    }
    protectAllFrames(context, wrappedWindow, interceptWindow, interceptAllFrames);
    return true;
}
function protectAllFrames(context, wrappedWindow, singleCallback, allCallback) {
    const changeWindowProperty = createChangeProperty(context);
    if (!changeWindowProperty) {
        return;
    }
    const api = {
        context,
        wrappedWindow,
        changeWindowProperty,
        singleCallback,
        allCallback,
        observe: null,
    };
    protectFrameProperties(api);
    protectDOMModifications(api);
    // MutationObserver to intercept iFrames while generating the DOM.
    api.observe = enableMutationObserver(api);
    // MutationObserver does not trigger fast enough when document.write is used
    protectDocumentWrite(api);
    protectWindowOpen(api);
}
function getWrapped(context) {
    return context && (context.wrappedJSObject || context);
}
function createChangeProperty(window) {
    const changeWindowProperty = function (object, name, type, changed) {
        const descriptor = Object.getOwnPropertyDescriptor(object, name);
        const original = descriptor[type];
        if (typeof changed === "function") {
            changed = createProxyFunction(window, original, changed);
        }
        changePropertyFunc(window, { object, name, type, changed });
    };
    return changeWindowProperty;
}
function createProxyFunction(context, original, replacement) {
    if (!changedToStrings.get(context)) {
        changedToStrings.set(context, true);
        const functionPrototype = getWrapped(context).Function.prototype;
        const toString = functionPrototype.toString;
        changePropertyFunc(context, {
            object: functionPrototype,
            name: "toString",
            type: "value",
            changed: createProxyFunction(context, toString, function () {
                return proxies.get(this) || toString.call(this);
            }),
        });
    }
    const handler = getWrapped(context).Object.create(null);
    handler.apply = (0, instrument_1.exportCustomFunction)(function (target, thisArgs, args) {
        try {
            return args.length
                ? replacement.call(thisArgs, ...args)
                : replacement.call(thisArgs);
        }
        catch (error) {
            try {
                return original.apply(thisArgs, args);
            }
            catch (error) {
                return target.apply(thisArgs, args);
            }
        }
    }, context, "");
    const proxy = new context.Proxy(original, handler);
    proxies.set(proxy, original.toString());
    return getWrapped(proxy);
}
function changePropertyFunc(_context, { object, name, type, changed }) {
    // Removed tracker for changed properties
    const descriptor = Object.getOwnPropertyDescriptor(object, name);
    descriptor[type] = changed;
    Object.defineProperty(object, name, descriptor);
}
function protectFrameProperties({ context, wrappedWindow, changeWindowProperty, singleCallback, }) {
    ["HTMLIFrameElement", "HTMLFrameElement"].forEach(function (constructorName) {
        const constructor = context[constructorName];
        const wrappedConstructor = wrappedWindow[constructorName];
        const contentWindowDescriptor = Object.getOwnPropertyDescriptor(constructor.prototype, "contentWindow");
        // TODO: Continue here!!!!
        const originalContentWindowGetter = contentWindowDescriptor.get;
        const contentWindowTemp = {
            get contentWindow() {
                const window = originalContentWindowGetter.call(this);
                if (window) {
                    singleCallback(window);
                }
                return window;
            },
        };
        changeWindowProperty(wrappedConstructor.prototype, "contentWindow", "get", Object.getOwnPropertyDescriptor(contentWindowTemp, "contentWindow").get);
        const contentDocumentDescriptor = Object.getOwnPropertyDescriptor(constructor.prototype, "contentDocument");
        const originalContentDocumentGetter = contentDocumentDescriptor.get;
        const contentDocumentTemp = {
            get contentDocument() {
                const document = originalContentDocumentGetter.call(this);
                if (document) {
                    singleCallback(document.defaultView);
                }
                return document;
            },
        };
        changeWindowProperty(wrappedConstructor.prototype, "contentDocument", "get", Object.getOwnPropertyDescriptor(contentDocumentTemp, "contentDocument")
            .get);
    });
}
function protectDOMModifications({ wrappedWindow, changeWindowProperty, allCallback, }) {
    [
        // useless as length could be obtained before the iframe is created and window.frames === window
        // {
        // 	object: wrappedWindow,
        // 	methods: [],
        // 	getters: ["length", "frames"],
        // 	setters: []
        // },
        {
            object: wrappedWindow.Node.prototype,
            methods: ["appendChild", "insertBefore", "replaceChild"],
            getters: [],
            setters: [],
        },
        {
            object: wrappedWindow.Element.prototype,
            methods: [
                "append",
                "prepend",
                "insertAdjacentElement",
                "insertAdjacentHTML",
                "insertAdjacentText",
                "replaceWith",
            ],
            getters: [],
            setters: ["innerHTML", "outerHTML"],
        },
    ].forEach(function (protectionDefinition) {
        const object = protectionDefinition.object;
        protectionDefinition.methods.forEach(function (method) {
            const descriptor = Object.getOwnPropertyDescriptor(object, method);
            const original = descriptor.value;
            changeWindowProperty(object, method, "value", class {
                [method]() {
                    const value = arguments.length
                        ? original.call(this, ...arguments)
                        : original.call(this);
                    allCallback();
                    return value;
                }
            }.prototype[method]);
        });
        protectionDefinition.getters.forEach(function (property) {
            const temp = {
                get [property]() {
                    const ret = this[property];
                    allCallback();
                    return ret;
                },
            };
            changeWindowProperty(object, property, "get", Object.getOwnPropertyDescriptor(temp, property).get);
        });
        protectionDefinition.setters.forEach(function (property) {
            const descriptor = Object.getOwnPropertyDescriptor(object, property);
            const setter = descriptor.set;
            const temp = {
                set(obj, _prop, value) {
                    const ret = setter.call(obj, value);
                    allCallback();
                    return ret;
                },
            };
            changeWindowProperty(object, property, "set", Object.getOwnPropertyDescriptor(temp, property).set);
        });
    });
}
function enableMutationObserver({ context, allCallback }) {
    const observer = new MutationObserver(allCallback);
    let observing = false;
    function observe() {
        if (!observing && context.document) {
            observer.observe(context.document, { subtree: true, childList: true });
            observing = true;
        }
    }
    observe();
    context.document.addEventListener("DOMContentLoaded", function () {
        if (observing) {
            observer.disconnect();
            observing = false;
        }
    });
    return observe;
}
function protectDocumentWrite({ context, wrappedWindow, changeWindowProperty, observe, allCallback, }) {
    const documentWriteDescriptorOnHTMLDocument = Object.getOwnPropertyDescriptor(wrappedWindow.HTMLDocument.prototype, "write");
    const documentWriteDescriptor = documentWriteDescriptorOnHTMLDocument ||
        Object.getOwnPropertyDescriptor(wrappedWindow.Document.prototype, "write");
    const documentWrite = documentWriteDescriptor.value;
    changeWindowProperty(documentWriteDescriptorOnHTMLDocument
        ? wrappedWindow.HTMLDocument.prototype
        : wrappedWindow.Document.prototype, "write", "value", function write(_markup) {
        for (let i = 0, l = arguments.length; i < l; i += 1) {
            const str = "" + arguments[i];
            // weird problem with waterfox and google docs
            const parts = str.match(/^\s*<!doctype/i) && !str.match(/frame/i)
                ? [str]
                : str.split(/(?=<)/);
            const length = parts.length;
            const scripts = context.document.getElementsByTagName("script");
            for (let i = 0; i < length; i += 1) {
                documentWrite.call(this, parts[i]);
                allCallback();
                if (scripts.length && scripts[scripts.length - 1].src) {
                    observe();
                }
            }
        }
    });
    const documentWritelnDescriptorOnHTMLDocument = Object.getOwnPropertyDescriptor(wrappedWindow.HTMLDocument.prototype, "writeln");
    const documentWritelnDescriptor = documentWritelnDescriptorOnHTMLDocument ||
        Object.getOwnPropertyDescriptor(wrappedWindow.Document.prototype, "writeln");
    const documentWriteln = documentWritelnDescriptor.value;
    changeWindowProperty(documentWritelnDescriptorOnHTMLDocument
        ? wrappedWindow.HTMLDocument.prototype
        : wrappedWindow.Document.prototype, "writeln", "value", function writeln(_markup) {
        for (let i = 0, l = arguments.length; i < l; i += 1) {
            const str = "" + arguments[i];
            const parts = str.split(/(?=<)/);
            const length = parts.length;
            const scripts = context.document.getElementsByTagName("script");
            for (let i = 0; i < length; i += 1) {
                documentWrite.call(this, parts[i]);
                allCallback();
                if (scripts.length && scripts[scripts.length - 1].src) {
                    observe();
                }
            }
        }
        documentWriteln.call(this, "");
    });
}
function protectWindowOpen({ context, wrappedWindow, changeWindowProperty, singleCallback, }) {
    const windowOpenDescriptor = Object.getOwnPropertyDescriptor(wrappedWindow, "open");
    const windowOpen = windowOpenDescriptor.value;
    const getDocument = Object.getOwnPropertyDescriptor(context, "document").get;
    changeWindowProperty(wrappedWindow, "open", "value", function open() {
        const newWindow = arguments.length
            ? windowOpen.call(this, ...arguments)
            : windowOpen.call(this);
        if (newWindow) {
            // if we use windowOpen from the normal window we see some SOP errors
            // BUT we need the unwrapped window...
            singleCallback(getDocument.call(newWindow).defaultView);
        }
        return newWindow;
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RlYWx0aC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9zdGVhbHRoL3N0ZWFsdGgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUEsWUFBWSxDQUFDOztBQUNiOzs7OERBRzhEO0FBRTlELDZDQUdzQjtBQUV0QixnQ0FBZ0M7QUFDaEMsTUFBTSxrQkFBa0IsR0FBRyxJQUFJLE9BQU8sRUFBRSxDQUFDO0FBQ3pDLE1BQU0sT0FBTyxHQUFHLElBQUksR0FBRyxFQUFFLENBQUM7QUFDMUIsTUFBTSxnQkFBZ0IsR0FBRyxJQUFJLE9BQU8sRUFBRSxDQUFDO0FBR3ZDLGlDQUFpQztBQUNqQyxDQUFDO0lBQ0Msd0NBQXdDO0lBQ3hDLElBQUksQ0FBQztRQUNILGVBQWUsQ0FBQyxNQUF3QixDQUFDLENBQUM7SUFDNUMsQ0FBQztJQUFDLE9BQU8sS0FBSyxFQUFFLENBQUM7UUFDZixPQUFPLENBQUMsR0FBRyxDQUFDLGtEQUFrRCxHQUFHLEtBQUssQ0FBQyxDQUFDO1FBQ3hFLE9BQU8sQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQzNCLENBQUM7SUFDRCx3Q0FBd0M7QUFDMUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztBQUVMLFNBQVMsZUFBZSxDQUFDLE9BQXVCO0lBQzlDLElBQUksVUFBVSxDQUFDO0lBQ2YsSUFBSSxDQUFDO1FBQ0gsVUFBVSxHQUFHLFVBQVUsQ0FBQyxPQUFPLENBQUMsQ0FBQztJQUNuQyxDQUFDO0lBQUMsT0FBTyxLQUFLLEVBQUUsQ0FBQztRQUNmLGdEQUFnRDtRQUNoRCxnREFBZ0Q7UUFDaEQsZ0VBQWdFO1FBQ2hFLE9BQU8sS0FBSyxDQUFDO0lBQ2YsQ0FBQztJQUNELE1BQU0sYUFBYSxHQUFHLFVBQVUsQ0FBQztJQUVqQyxJQUFJLGtCQUFrQixDQUFDLEdBQUcsQ0FBQyxhQUFhLENBQUMsRUFBRSxDQUFDO1FBQzFDLGlEQUFpRDtRQUNqRCxPQUFPLEtBQUssQ0FBQztJQUNm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

/***/ }

}]);
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3JjX3N0ZWFsdGhfc3RlYWx0aF90cy5qcyIsIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7QUFBYTtBQUNiLDhDQUE2QyxFQUFFLGFBQWEsRUFBQztBQUM3RCwyQkFBMkI7QUFDM0IsNkJBQTZCO0FBQzdCLHFCQUFxQjtBQUNyQjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxLQUFLO0FBQ0w7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLG9CQUFvQixrQkFBa0I7QUFDdEM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSwyQ0FBMkMsMnpJOzs7Ozs7Ozs7O0FDckg5QjtBQUNiLDhDQUE2QyxFQUFFLGFBQWEsRUFBQztBQUM3RCx1QkFBdUI7QUFDdkIsNEJBQTRCO0FBQzVCLG1CQUFtQixtQkFBTyxDQUFDLDZDQUFZO0FBQ3ZDLGdCQUFnQixtQkFBTyxDQUFDLHVDQUFTO0FBQ2pDO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsU0FBUztBQUNUO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLGdDQUFnQztBQUNoQztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSw0REFBNEQ7QUFDNUQ7QUFDQSx3Q0FBd0M7QUFDeEM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsQ0FBQztBQUNEO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsMERBQTBEO0FBQzFEO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxvQkFBb0IsWUFBWTtBQUNoQztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxlQUFlLG1EQUFtRDtBQUNsRTtBQUNBO0FBQ0E7QUFDQTtBQUNBLG1CQUFtQixpREFBaUQ7QUFDcEU7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxxQkFBcUI7QUFDckI7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxLQUFLO0FBQ0w7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFNBQVM7QUFDVDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsMENBQTBDLDhCQUE4QjtBQUN4RTtBQUNBO0FBQ0E7QUFDQSxhQUFhO0FBQ2I7QUFDQTtBQUNBO0FBQ0EsOERBQThELGtCQUFrQjtBQUNoRjtBQUNBO0FBQ0EsNEJBQTRCLHNCQUFzQjtBQUNsRDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLGFBQWE7QUFDYjtBQUNBO0FBQ0E7QUFDQSwyQ0FBMkMsK3F2Qjs7Ozs7Ozs7OztBQzFvQjlCO0FBQ2IsOENBQTZDLEVBQUUsYUFBYSxFQUFDO0FBQzdELGlDQUFpQztBQUNqQyxpQ0FBaUM7QUFDakM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsU0FBUztBQUNULEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFNBQVM7QUFDVCxLQUFLO0FBQ0w7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsU0FBUztBQUNULEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsU0FBUztBQUNULEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFNBQVM7QUFDVCxLQUFLO0FBQ0w7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFNBQVM7QUFDVCxLQUFLO0FBQ0w7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNB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QTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsYUFBYTtBQUNiLFNBQVM7QUFDVDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxpQkFBaUI7QUFDakI7QUFDQTtBQUNBLFNBQVM7QUFDVDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsaUJBQWlCO0FBQ2pCO0FBQ0E7QUFDQSxTQUFTO0FBQ1QsS0FBSztBQUNMO0FBQ0Esa0NBQWtDLHNCQUFzQjtBQUN4RDtBQUNBO0FBQ0E7QUFDQTtBQUNBLGlEQUFpRCxnQ0FBZ0M7QUFDakY7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsS0FBSztBQUNMO0FBQ0E7QUFDQSxnQ0FBZ0MscUVBQXFFO0FBQ3JHO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsOENBQThDLE9BQU87QUFDckQ7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSw0QkFBNEIsWUFBWTtBQUN4QztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLEtBQUs7QUFDTDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLDhDQUE4QyxPQUFPO0FBQ3JEO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsNEJBQTRCLFlBQVk7QUFDeEM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLEtBQUs7QUFDTDtBQUNBLDZCQUE2QiwrREFBK0Q7QUFDNUY7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxLQUFLO0FBQ0w7QUFDQSwyQ0FBMkMsKzZiIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi9zcmMvc3RlYWx0aC9lcnJvci50cyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uL3NyYy9zdGVhbHRoL2luc3RydW1lbnQudHMiLCJ3ZWJwYWNrOi8vQG9wZW53cG0vd2ViZXh0LWZpcmVmb3gvLi9zcmMvc3RlYWx0aC9zZXR0aW5ncy50cyIsIndlYnBhY2s6Ly9Ab3BlbndwbS93ZWJleHQtZmlyZWZveC8uL3NyYy9zdGVhbHRoL3N0ZWFsdGgudHMiXSwic291cmNlc0NvbnRlbnQiOlsiXCJ1c2Ugc3RyaWN0XCI7XG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHsgdmFsdWU6IHRydWUgfSk7XG5leHBvcnRzLmdlbmVyYXRlRXJyb3JPYmplY3QgPSBnZW5lcmF0ZUVycm9yT2JqZWN0O1xuZXhwb3J0cy5nZXRCZWdpbk9mU2NyaXB0Q2FsbHMgPSBnZXRCZWdpbk9mU2NyaXB0Q2FsbHM7XG5leHBvcnRzLmdldFN0YWNrVHJhY2UgPSBnZXRTdGFja1RyYWNlO1xuLypcbiAqIEZ1bmN0aW9uYWxpdHkgdG8gZ2VuZXJhdGUgZXJyb3Igb2JqZWN0c1xuICovXG5mdW5jdGlvbiBnZW5lcmF0ZUVycm9yT2JqZWN0KGVyciwgY29udGV4dCA9IHVuZGVmaW5lZCkge1xuICAgIC8vIFRPRE86IFBhc3MgY29udGV4dFxuICAgIGNvbnRleHQgPSBjb250ZXh0ICE9PSB1bmRlZmluZWQgPyBjb250ZXh0IDogd2luZG93O1xuICAgIGNvbnN0IGNsZWFuZWQgPSBjbGVhbkVycm9yU3RhY2soZXJyLnN0YWNrKTtcbiAgICBjb25zdCBzdGFjayA9IHNwbGl0U3RhY2soY2xlYW5lZCk7XG4gICAgY29uc3QgbGluZUluZm8gPSBnZXRMaW5lSW5mbyhzdGFjayk7XG4gICAgY29uc3QgZmlsZU5hbWUgPSBnZXRGaWxlTmFtZShzdGFjayk7XG4gICAgbGV0IGZha2VFcnJvcjtcbiAgICB0cnkge1xuICAgICAgICAvLyBmYWtlIHR5cGUsIG1lc3NhZ2UsIGZpbGVuYW1lLCBjb2x1bW4gYW5kIGxpbmVcbiAgICAgICAgLy8gY29uc3QgcHJvcGVydHlOYW1lID0gXCJzdGFja1wiO1xuICAgICAgICBmYWtlRXJyb3IgPSBuZXcgY29udGV4dC53cmFwcGVkSlNPYmplY3RbZXJyLm5hbWVdKGVyci5tZXNzYWdlLCBmaWxlTmFtZSk7XG4gICAgICAgIGZha2VFcnJvci5saW5lTnVtYmVyID0gbGluZUluZm8ubGluZU51bWJlcjtcbiAgICAgICAgZmFrZUVycm9yLmNvbHVtbk51bWJlciA9IGxpbmVJbmZvLmNvbHVtbk51bWJlcjtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiRVJST1IgY3JlYXRpb24gZmFpbGVkLiBFcnJvciB3YXM6XCIgKyBlcnJvcik7XG4gICAgfVxuICAgIHJldHVybiBmYWtlRXJyb3I7XG59XG4vKlxuICogVHJpbXMgdHJhY2VzIGZyb20gdGhlIHN0YWNrLCB3aGljaCBjb250YWluIHRoZSBleHRpb25zaW9uIElEXG4gKi9cbmZ1bmN0aW9uIGNsZWFuRXJyb3JTdGFjayhzdGFjaykge1xuICAgIGNvbnN0IGV4dGVuc2lvbklEID0gYnJvd3Nlci5ydW50aW1lLmdldFVSTChcIlwiKTtcbiAgICBjb25zdCBsaW5lcyA9IHR5cGVvZiBzdGFjayAhPT0gXCJzdHJpbmdcIiA/IHN0YWNrIDogc3BsaXRTdGFjayhzdGFjayk7XG4gICAgbGluZXMuZm9yRWFjaCgobGluZSkgPT4ge1xuICAgICAgICBpZiAobGluZS5pbmNsdWRlcyhleHRlbnNpb25JRCkpIHtcbiAgICAgICAgICAgIHN0YWNrID0gc3RhY2sucmVwbGFjZShsaW5lICsgXCJcXG5cIiwgXCJcIik7XG4gICAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gc3RhY2s7XG59XG4vKlxuICogUHJvdmlkZXMgdGhlIGluZGV4IHRoZSBmaXJzdCBjYWxsIG91dHNpZGUgb2YgdGhlIGV4dGVuc2lvblxuICovXG5mdW5jdGlvbiBnZXRCZWdpbk9mU2NyaXB0Q2FsbHMoc3RhY2spIHtcbiAgICBjb25zdCBleHRlbnNpb25JRCA9IGJyb3dzZXIucnVudGltZS5nZXRVUkwoXCJcIik7XG4gICAgY29uc3QgbGluZXMgPSB0eXBlb2Ygc3RhY2sgIT09IFwic3RyaW5nXCIgPyBzdGFjayA6IHNwbGl0U3RhY2soc3RhY2spO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgbGluZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgaWYgKCFsaW5lc1tpXS5pbmNsdWRlcyhleHRlbnNpb25JRCkpIHtcbiAgICAgICAgICAgIHJldHVybiBpO1xuICAgICAgICB9XG4gICAgfVxuICAgIHJldHVybiAtMTtcbn1cbi8qXG4gKiBHZXQgdGhlIHN0YWNrIGFzIGFycmF5XG4gKi9cbmZ1bmN0aW9uIHNwbGl0U3RhY2soc3RhY2spIHtcbiAgICByZXR1cm4gc3RhY2suc3BsaXQoXCJcXG5cIikubWFwKGZ1bmN0aW9uIChsaW5lKSB7XG4gICAgICAgIHJldHVybiBsaW5lLnRyaW0oKTtcbiAgICB9KTtcbn1cbi8qXG4gKiBSZXRyaWV2ZXMgbGluZSBhbmQgY29sdW1uIGluZm9ybWF0aW9uIG9mIHRoZSBmdW5jdGlvblxuICogY2FsbGluZyBiZWZvcmUgdGhlIGV4dGVuc2lvbiB3YXMgaW52b2x2ZWRcbiAqL1xuZnVuY3Rpb24gZ2V0TGluZUluZm8oc3RhY2spIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBzdGFja1swXTtcbiAgICBjb25zdCBtYXRjaGVzID0gWy4uLmZpcnN0TGluZS5tYXRjaEFsbChcIjpcIildO1xuICAgIGNvbnN0IGNvbHVtbiA9IGZpcnN0TGluZS5zbGljZShtYXRjaGVzW21hdGNoZXMubGVuZ3RoIC0gMV0uaW5kZXggKyAxLCBmaXJzdExpbmUubGVuZ3RoKTtcbiAgICBjb25zdCBsaW5lID0gZmlyc3RMaW5lLnNsaWNlKG1hdGNoZXNbbWF0Y2hlcy5sZW5ndGggLSAyXS5pbmRleCArIDEsIG1hdGNoZXNbbWF0Y2hlcy5sZW5ndGggLSAxXS5pbmRleCk7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbGluZU51bWJlcjogbGluZSxcbiAgICAgICAgY29sdW1uTnVtYmVyOiBjb2x1bW4sXG4gICAgfTtcbn1cbi8qXG4gKiBSZXRyaWV2ZXMgZmlsZSBuYW1lIG9mIHRoZSBmdW5jdGlvblxuICogdGhhdCBjYWxsZWQgYmVmb3JlIHRoZSBleHRlbnNpb24gZ290IGludm9sdmVkXG4gKi9cbmZ1bmN0aW9uIGdldEZpbGVOYW1lKHN0YWNrKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gc3RhY2tbMF07XG4gICAgY29uc3QgbWF0Y2hlc19hdCA9IFsuLi5maXJzdExpbmUubWF0Y2hBbGwoXCJAXCIpXTtcbiAgICBjb25zdCBtYXRjaGVzX2NvbG9uID0gWy4uLmZpcnN0TGluZS5tYXRjaEFsbChcIjpcIildO1xuICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UobWF0Y2hlc19hdFttYXRjaGVzX2F0Lmxlbmd0aCAtIDFdLmluZGV4ICsgMSwgbWF0Y2hlc19jb2xvblttYXRjaGVzX2NvbG9uLmxlbmd0aCAtIDJdLmluZGV4KTtcbn1cbi8vIGZ1bmN0aW9uIGdldE9yaWdpbkZyb21TdGFja1RyYWNlKGVyciwgaW5jbHVkZVN0YWNrKXtcbi8vICAgY29uc29sZS5sb2coZXJyLnN0YWNrKTtcbi8vICAgY29uc3Qgc3RhY2sgPSBzcGxpdFN0YWNrKGVyci5zdGFjayk7XG4vLyAgIGNvbnN0IGxpbmVJbmZvID0gZ2V0TGluZUluZm8oc3RhY2spO1xuLy8gICBjb25zdCBmaWxlTmFtZSA9IGdldEZpbGVOYW1lKHN0YWNrKTtcbi8vICAgY29uc3QgY2FsbFNpdGUgPSBzdGFja1sxXTtcbi8vICAgY29uc3QgY2FsbFNpdGVQYXJ0cyA9IGNhbGxTaXRlLnNwbGl0KFwiQFwiKTtcbi8vICAgY29uc3QgZnVuY05hbWUgPSBjYWxsU2l0ZVBhcnRzWzBdIHx8IFwiXCI7XG4vLyAgIGNvbnN0IGl0ZW1zID0gcnNwbGl0KGNhbGxTaXRlUGFydHNbMV0sIFwiOlwiLCAyKTtcbi8vICAgY29uc3Qgc2NyaXB0RmlsZU5hbWUgPSBpdGVtc1tpdGVtcy5sZW5ndGggLSAzXSB8fCBcIlwiO1xuLy8gICBjb25zdCBjYWxsQ29udGV4dCA9IHtcbi8vICAgICBzY3JpcHRVcmwsXG4vLyAgICAgc2NyaXB0TGluZTogbGluZUluZm8ubGluZU51bWJlcixcbi8vICAgICBzY3JpcHRDb2w6IGxpbmVJbmZvLmNvbHVtbk51bWJlcixcbi8vICAgICBmdW5jTmFtZSxcbi8vICAgICBzY3JpcHRMb2NFdmFsLFxuLy8gICAgIGNhbGxTdGFjazogaW5jbHVkZVN0YWNrID8gdHJhY2Uuc2xpY2UoMykuam9pbihcIlxcblwiKS50cmltKCkgOiBcIlwiLFxuLy8gICB9O1xuLy8gfVxuLy8gSGVscGVyIHRvIGdldCBvcmlnaW5hdGluZyBzY3JpcHQgdXJsc1xuLy8gTGVnYWN5IGNvZGVcbmZ1bmN0aW9uIGdldFN0YWNrVHJhY2UoKSB7XG4gICAgbGV0IHN0YWNrO1xuICAgIHRyeSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcigpO1xuICAgIH1cbiAgICBjYXRjaCAoZXJyKSB7XG4gICAgICAgIHN0YWNrID0gZXJyLnN0YWNrO1xuICAgIH1cbiAgICByZXR1cm4gc3RhY2s7XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2laWEp5YjNJdWFuTWlMQ0p6YjNWeVkyVlNiMjkwSWpvaUlpd2ljMjkxY21ObGN5STZXeUl1TGk4dUxpOHVMaTl6Y21NdmMzUmxZV3gwYUM5bGNuSnZjaTUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pT3p0QlFYZEpVeXhyUkVGQmJVSTdRVUZCUlN4elJFRkJjVUk3UVVGQlJTeHpRMEZCWVR0QlFYaEpiRVU3TzBkQlJVYzdRVUZEU0N4VFFVRlRMRzFDUVVGdFFpeERRVU14UWl4SFFVRjNSQ3hGUVVONFJDeFBRVUZQTEVkQlFVY3NVMEZCVXp0SlFVVnVRaXh4UWtGQmNVSTdTVUZEY2tJc1QwRkJUeXhIUVVGSExFOUJRVThzUzBGQlN5eFRRVUZUTEVOQlFVTXNRMEZCUXl4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRExFTkJRVU1zVFVGQlRTeERRVUZETzBsQlEyNUVMRTFCUVUwc1QwRkJUeXhIUVVGSExHVkJRV1VzUTBGQlF5eEhRVUZITEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNN1NVRkRNME1zVFVGQlRTeExRVUZMTEVkQlFVY3NWVUZCVlN4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRE8wbEJRMnhETEUxQlFVMHNVVUZCVVN4SFFVRkhMRmRCUVZjc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dEpRVU53UXl4TlFVRk5MRkZCUVZFc1IwRkJSeXhYUVVGWExFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTTdTVUZEY0VNc1NVRkJTU3hUUVVGcFJDeERRVUZETzBsQlEzUkVMRWxCUVVrc1EwRkJRenRSUVVOSUxHZEVRVUZuUkR0UlFVTm9SQ3huUTBGQlowTTdVVUZEYUVNc1UwRkJVeXhIUVVGSExFbEJRVWtzVDBGQlR5eERRVUZETEdWQlFXVXNRMEZCUXl4SFFVRkhMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU1zUjBGQlJ5eERRVUZETEU5QlFVOHNSVUZCUlN4UlFVRlJMRU5CUVVNc1EwRkJRenRSUVVONlJTeFRRVUZUTEVOQlFVTXNWVUZCVlN4SFFVRkhMRkZCUVZFc1EwRkJReXhWUVVGVkxFTkJRVU03VVVGRE0wTXNVMEZCVXl4RFFVRkRMRmxCUVZrc1IwRkJSeXhSUVVGUkxFTkJRVU1zV1VGQldTeERRVUZETzBsQlEycEVMRU5CUVVNN1NVRkJReXhQUVVGUExFdEJRVXNzUlVGQlJTeERRVUZETzFGQlEyWXNUMEZCVHl4RFFVRkRMRWRCUVVjc1EwRkJReXh0UTBGQmJVTXNSMEZCUnl4TFFVRkxMRU5CUVVNc1EwRkJRenRKUVVNelJDeERRVUZETzBsQlEwUXNUMEZCVHl4VFFVRlRMRU5CUVVNN1FVRkRia0lzUTBGQlF6dEJRVVZFT3p0SFFVVkhPMEZCUTBnc1UwRkJVeXhsUVVGbExFTkJRVU1zUzBGQlN6dEpRVU0xUWl4TlFVRk5MRmRCUVZjc1IwRkJSeXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETEUxQlFVMHNRMEZCUXl4RlFVRkZMRU5CUVVNc1EwRkJRenRKUVVNdlF5eE5RVUZOTEV0QlFVc3NSMEZCUnl4UFFVRlBMRXRCUVVzc1MwRkJTeXhSUVVGUkxFTkJRVU1zUTBGQlF5eERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRMRU5CUVVNc1ZVRkJWU3hEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETzBsQlEzQkZMRXRCUVVzc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF5eEpRVUZKTEVWQlFVVXNSVUZCUlR0UlFVTnlRaXhKUVVGSkxFbEJRVWtzUTBGQlF5eFJRVUZSTEVOQlFVTXNWMEZCVnl4RFFVRkRMRVZCUVVVc1EwRkJRenRaUVVNdlFpeExRVUZMTEVkQlFVY3NTMEZCU3l4RFFVRkRMRTlCUVU4c1EwRkJReXhKUVVGSkxFZEJRVWNzU1VGQlNTeEZRVUZGTEVWQlFVVXNRMEZCUXl4RFFVRkRPMUZCUTNwRExFTkJRVU03U1VGRFNDeERRVUZETEVOQlFVTXNRMEZCUXp0SlFVTklMRTlCUVU4c1MwRkJTeXhEUVVGRE8wRkJRMllzUTBGQlF6dEJRVVZFT3p0SFFVVkhPMEZCUTBnc1UwRkJVeXh4UWtGQmNVSXNRMEZCUXl4TFFVRkxPMGxCUTJ4RExFMUJRVTBzVjBGQlZ5eEhRVUZITEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNc1RVRkJUU3hEUVVGRExFVkJRVVVzUTBGQlF5eERRVUZETzBsQlF5OURMRTFCUVUwc1MwRkJTeXhIUVVGSExFOUJRVThzUzBGQlN5eExRVUZMTEZGQlFWRXNRMEZCUXl4RFFVRkRMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU1zUTBGQlF5eFZRVUZWTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNN1NVRkRjRVVzUzBGQlN5eEpRVUZKTEVOQlFVTXNSMEZCUnl4RFFVRkRMRVZCUVVVc1EwRkJReXhIUVVGSExFdEJRVXNzUTBGQlF5eE5RVUZOTEVWQlFVVXNRMEZCUXl4RlFVRkZMRVZCUVVVc1EwRkJRenRSUVVOMFF5eEpRVUZKTEVOQlFVTXNTMEZCU3l4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExGRkJRVkVzUTBGQlF5eFhRVUZYTEVOQlFVTXNSVUZCUlN4RFFVRkRPMWxCUTNCRExFOUJRVThzUTBGQlF5eERRVUZETzFGQlExZ3NRMEZCUXp0SlFVTklMRU5CUVVNN1NVRkRSQ3hQUVVGUExFTkJRVU1zUTBGQlF5eERRVUZETzBGQlExb3NRMEZCUXp0QlFVVkVPenRIUVVWSE8wRkJRMGdzVTBGQlV5eFZRVUZWTEVOQlFVTXNTMEZCU3p0SlFVTjJRaXhQUVVGUExFdEJRVXNzUTBGQlF5eExRVUZMTEVOQlFVTXNTVUZCU1N4RFFVRkRMRU5CUVVNc1IwRkJSeXhEUVVGRExGVkJRVlVzU1VGQlNUdFJRVU42UXl4UFFVRlBMRWxCUVVrc1EwRkJReXhKUVVGSkxFVkJRVVVzUTBGQlF6dEpRVU55UWl4RFFVRkRMRU5CUVVNc1EwRkJRenRCUVVOTUxFTkJRVU03UVVGRlJEczdPMGRCUjBjN1FVRkRTQ3hUUVVGVExGZEJRVmNzUTBGQlF5eExRVUZMTzBsQlEzaENMRTFCUVUwc1UwRkJVeXhIUVVGSExFdEJRVXNzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXp0SlFVTXpRaXhOUVVGTkxFOUJRVThzUjBGQlJ5eERRVUZETEVkQlFVY3NVMEZCVXl4RFFVRkRMRkZCUVZFc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETzBsQlF6ZERMRTFCUVUwc1RVRkJUU3hIUVVGSExGTkJRVk1zUTBGQlF5eExRVUZMTEVOQlF6VkNMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRVU1zVFVGQlRTeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRXRCUVVzc1IwRkJSeXhEUVVGRExFVkJRM0pETEZOQlFWTXNRMEZCUXl4TlFVRk5MRU5CUTJwQ0xFTkJRVU03U1VGRFJpeE5RVUZOTEVsQlFVa3NSMEZCUnl4VFFVRlRMRU5CUVVNc1MwRkJTeXhEUVVNeFFpeFBRVUZQTEVOQlFVTXNUMEZCVHl4RFFVRkRMRTFCUVUwc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eExRVUZMTEVkQlFVY3NRMEZCUXl4RlFVTnlReXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhMUVVGTExFTkJRMnhETEVOQlFVTTdTVUZEUml4UFFVRlBPMUZCUTB3c1ZVRkJWU3hGUVVGRkxFbEJRVWs3VVVGRGFFSXNXVUZCV1N4RlFVRkZMRTFCUVUwN1MwRkRja0lzUTBGQlF6dEJRVU5LTEVOQlFVTTdRVUZGUkRzN08wZEJSMGM3UVVGRFNDeFRRVUZUTEZkQlFWY3NRMEZCUXl4TFFVRkxPMGxCUTNoQ0xFMUJRVTBzVTBGQlV5eEhRVUZITEV0QlFVc3NRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRKUVVNelFpeE5RVUZOTEZWQlFWVXNSMEZCUnl4RFFVRkRMRWRCUVVjc1UwRkJVeXhEUVVGRExGRkJRVkVzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRPMGxCUTJoRUxFMUJRVTBzWVVGQllTeEhRVUZITEVOQlFVTXNSMEZCUnl4VFFVRlRMRU5CUVVNc1VVRkJVU3hEUVVGRExFZEJRVWNzUTBGQlF5eERRVUZETEVOQlFVTTdTVUZEYmtRc1QwRkJUeXhUUVVGVExFTkJRVU1zUzBGQlN5eERRVU53UWl4VlFVRlZMRU5CUVVNc1ZVRkJWU3hEUVVGRExFMUJRVTBzUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4TFFVRkxMRWRCUVVjc1EwRkJReXhGUVVNelF5eGhRVUZoTEVOQlFVTXNZVUZCWVN4RFFVRkRMRTFCUVUwc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF5eExRVUZMTEVOQlF6bERMRU5CUVVNN1FVRkRTaXhEUVVGRE8wRkJSVVFzZFVSQlFYVkVPMEZCUTNaRUxEUkNRVUUwUWp0QlFVVTFRaXg1UTBGQmVVTTdRVUZEZWtNc2VVTkJRWGxETzBGQlEzcERMSGxEUVVGNVF6dEJRVVY2UXl3clFrRkJLMEk3UVVGREwwSXNLME5CUVN0RE8wRkJReTlETERaRFFVRTJRenRCUVVNM1F5eHZSRUZCYjBRN1FVRkRjRVFzTUVSQlFUQkVPMEZCUlRGRUxEQkNRVUV3UWp0QlFVTXhRaXhwUWtGQmFVSTdRVUZEYWtJc2RVTkJRWFZETzBGQlEzWkRMSGREUVVGM1F6dEJRVU40UXl4blFrRkJaMEk3UVVGRGFFSXNjVUpCUVhGQ08wRkJRM0pDTEhWRlFVRjFSVHRCUVVOMlJTeFBRVUZQTzBGQlJWQXNTVUZCU1R0QlFVVktMSGREUVVGM1F6dEJRVU40UXl4alFVRmpPMEZCUTJRc1UwRkJVeXhoUVVGaE8wbEJRM0JDTEVsQlFVa3NTMEZCU3l4RFFVRkRPMGxCUlZZc1NVRkJTU3hEUVVGRE8xRkJRMGdzVFVGQlRTeEpRVUZKTEV0QlFVc3NSVUZCUlN4RFFVRkRPMGxCUTNCQ0xFTkJRVU03U1VGQlF5eFBRVUZQTEVkQlFVY3NSVUZCUlN4RFFVRkRPMUZCUTJJc1MwRkJTeXhIUVVGSExFZEJRVWNzUTBGQlF5eExRVUZMTEVOQlFVTTdTVUZEY0VJc1EwRkJRenRKUVVWRUxFOUJRVThzUzBGQlN5eERRVUZETzBGQlEyWXNRMEZCUXlKOSIsIlwidXNlIHN0cmljdFwiO1xuT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFwiX19lc01vZHVsZVwiLCB7IHZhbHVlOiB0cnVlIH0pO1xuZXhwb3J0cy5zdGFydEluc3RydW1lbnQgPSBzdGFydEluc3RydW1lbnQ7XG5leHBvcnRzLmV4cG9ydEN1c3RvbUZ1bmN0aW9uID0gZXhwb3J0Q3VzdG9tRnVuY3Rpb247XG5jb25zdCBzZXR0aW5nc18xID0gcmVxdWlyZShcIi4vc2V0dGluZ3NcIik7XG5jb25zdCBlcnJvcl8xID0gcmVxdWlyZShcIi4vZXJyb3JcIik7XG4vKiogKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqXG4gKiBPcGVuV1BNIGxlZ2FjeSBjb2RlXG4gKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioqL1xuLy8gQ291bnRlciB0byBjYXAgIyBvZiBjYWxscyBsb2dnZWQgZm9yIGVhY2ggc2NyaXB0L2FwaSBjb21iaW5hdGlvblxuY29uc3QgbWF4TG9nQ291bnQgPSA1MDA7XG4vLyBsb2dDb3VudGVyXG5jb25zdCBsb2dDb3VudGVyID0ge307XG4vLyBQcmV2ZW50IGxvZ2dpbmcgb2YgZ2V0cyBhcmlzaW5nIGZyb20gbG9nZ2luZ1xubGV0IGluTG9nID0gZmFsc2U7XG4vLyBUbyBrZWVwIHRyYWNrIG9mIHRoZSBvcmlnaW5hbCBvcmRlciBvZiBldmVudHNcbmxldCBvcmRpbmFsID0gMDtcbi8vIE9wdGlvbnMgZm9yIEpTT3BlcmF0aW9uXG5jb25zdCBKU09wZXJhdGlvbiA9IHtcbiAgICBjYWxsOiBcImNhbGxcIixcbiAgICBnZXQ6IFwiZ2V0XCIsXG4gICAgZ2V0X2ZhaWxlZDogXCJnZXQoZmFpbGVkKVwiLFxuICAgIGdldF9mdW5jdGlvbjogXCJnZXQoZnVuY3Rpb24pXCIsXG4gICAgc2V0OiBcInNldFwiLFxuICAgIHNldF9mYWlsZWQ6IFwic2V0KGZhaWxlZClcIixcbiAgICBzZXRfcHJldmVudGVkOiBcInNldChwcmV2ZW50ZWQpXCIsXG59O1xuLy8gZnJvbSBodHRwOi8vc3RhY2tvdmVyZmxvdy5jb20vYS81MjAyMTg1XG5mdW5jdGlvbiByc3BsaXQoc291cmNlLCBzZXAsIG1heHNwbGl0KSB7XG4gICAgY29uc3Qgc3BsaXQgPSBzb3VyY2Uuc3BsaXQoc2VwKTtcbiAgICByZXR1cm4gbWF4c3BsaXRcbiAgICAgICAgPyBbc3BsaXQuc2xpY2UoMCwgLW1heHNwbGl0KS5qb2luKHNlcCldLmNvbmNhdChzcGxpdC5zbGljZSgtbWF4c3BsaXQpKVxuICAgICAgICA6IHNwbGl0O1xufVxuLy8gSGVscGVyIGZvciBKU09OaWZ5aW5nIG9iamVjdHNcbmZ1bmN0aW9uIHNlcmlhbGl6ZU9iamVjdChvYmplY3QsIFxuLy8gc3RyaW5naWZ5RnVuY3Rpb25zOiBib29sZWFuID0gZmFsc2UsXG5zdHJpbmdpZnlGdW5jdGlvbnMpIHtcbiAgICAvLyBIYW5kbGUgcGVybWlzc2lvbnMgZXJyb3JzXG4gICAgdHJ5IHtcbiAgICAgICAgaWYgKG9iamVjdCA9PT0gbnVsbCkge1xuICAgICAgICAgICAgcmV0dXJuIFwibnVsbFwiO1xuICAgICAgICB9XG4gICAgICAgIGlmICh0eXBlb2Ygb2JqZWN0ID09PSBcImZ1bmN0aW9uXCIpIHtcbiAgICAgICAgICAgIHJldHVybiBzdHJpbmdpZnlGdW5jdGlvbnMgPyBvYmplY3QudG9TdHJpbmcoKSA6IFwiRlVOQ1RJT05cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAodHlwZW9mIG9iamVjdCAhPT0gXCJvYmplY3RcIikge1xuICAgICAgICAgICAgcmV0dXJuIG9iamVjdDtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBzZWVuT2JqZWN0cyA9IFtdO1xuICAgICAgICByZXR1cm4gSlNPTi5zdHJpbmdpZnkob2JqZWN0LCBmdW5jdGlvbiAoa2V5LCB2YWx1ZSkge1xuICAgICAgICAgICAgaWYgKHZhbHVlID09PSBudWxsKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIFwibnVsbFwiO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHN0cmluZ2lmeUZ1bmN0aW9ucyA/IHZhbHVlLnRvU3RyaW5nKCkgOiBcIkZVTkNUSU9OXCI7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcIm9iamVjdFwiKSB7XG4gICAgICAgICAgICAgICAgLy8gUmVtb3ZlIHdyYXBwaW5nIG9uIGNvbnRlbnQgb2JqZWN0c1xuICAgICAgICAgICAgICAgIGlmIChcIndyYXBwZWRKU09iamVjdFwiIGluIHZhbHVlKSB7XG4gICAgICAgICAgICAgICAgICAgIHZhbHVlID0gdmFsdWUud3JhcHBlZEpTT2JqZWN0O1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAvLyBTZXJpYWxpemUgRE9NIGVsZW1lbnRzXG4gICAgICAgICAgICAgICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgSFRNTEVsZW1lbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGdldFBhdGhUb0RvbUVsZW1lbnQodmFsdWUpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAvLyBQcmV2ZW50IHNlcmlhbGl6YXRpb24gY3ljbGVzXG4gICAgICAgICAgICAgICAgaWYgKGtleSA9PT0gXCJcIiB8fCBzZWVuT2JqZWN0cy5pbmRleE9mKHZhbHVlKSA8IDApIHtcbiAgICAgICAgICAgICAgICAgICAgc2Vlbk9iamVjdHMucHVzaCh2YWx1ZSk7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiB0eXBlb2YgdmFsdWU7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcmV0dXJuIHZhbHVlO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogU0VSSUFMSVpBVElPTiBFUlJPUjogXCIgKyBlcnJvcik7XG4gICAgICAgIHJldHVybiBcIlNFUklBTElaQVRJT04gRVJST1I6IFwiICsgZXJyb3I7XG4gICAgfVxufVxuLy8gUm91Z2ggaW1wbGVtZW50YXRpb25zIG9mIE9iamVjdC5nZXRQcm9wZXJ0eURlc2NyaXB0b3IgYW5kIE9iamVjdC5nZXRQcm9wZXJ0eU5hbWVzXG4vLyBTZWUgaHR0cDovL3dpa2kuZWNtYXNjcmlwdC5vcmcvZG9rdS5waHA/aWQ9aGFybW9ueTpleHRlbmRlZF9vYmplY3RfYXBpXG5PYmplY3QuZ2V0UHJvcGVydHlEZXNjcmlwdG9yID0gZnVuY3Rpb24gKHN1YmplY3QsIG5hbWUpIHtcbiAgICBpZiAoc3ViamVjdCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcIkNhbid0IGdldCBwcm9wZXJ0eSBkZXNjcmlwdG9yIGZvciB1bmRlZmluZWRcIik7XG4gICAgfVxuICAgIGxldCBwZCA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3Ioc3ViamVjdCwgbmFtZSk7XG4gICAgbGV0IHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHN1YmplY3QpO1xuICAgIHdoaWxlIChwZCA9PT0gdW5kZWZpbmVkICYmIHByb3RvICE9PSBudWxsKSB7XG4gICAgICAgIHBkID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihwcm90bywgbmFtZSk7XG4gICAgICAgIHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHByb3RvKTtcbiAgICB9XG4gICAgcmV0dXJuIHBkO1xufTtcbmZ1bmN0aW9uIHVwZGF0ZUNvdW50ZXJBbmRDaGVja0lmT3ZlcihzY3JpcHRVcmwsIHN5bWJvbCkge1xuICAgIGNvbnN0IGtleSA9IHNjcmlwdFVybCArIFwifFwiICsgc3ltYm9sO1xuICAgIGlmIChrZXkgaW4gbG9nQ291bnRlciAmJiBsb2dDb3VudGVyW2tleV0gPj0gbWF4TG9nQ291bnQpIHtcbiAgICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIGVsc2UgaWYgKCEoa2V5IGluIGxvZ0NvdW50ZXIpKSB7XG4gICAgICAgIGxvZ0NvdW50ZXJba2V5XSA9IDE7XG4gICAgfVxuICAgIGVsc2Uge1xuICAgICAgICBsb2dDb3VudGVyW2tleV0gKz0gMTtcbiAgICB9XG4gICAgcmV0dXJuIGZhbHNlO1xufVxuLy8gUmVjdXJzaXZlbHkgZ2VuZXJhdGVzIGEgcGF0aCBmb3IgYW4gZWxlbWVudFxuZnVuY3Rpb24gZ2V0UGF0aFRvRG9tRWxlbWVudChlbGVtZW50LCB2aXNpYmlsaXR5QXR0ciA9IGZhbHNlKSB7XG4gICAgaWYgKGVsZW1lbnQgPT09IGRvY3VtZW50LmJvZHkpIHtcbiAgICAgICAgcmV0dXJuIGVsZW1lbnQudGFnTmFtZTtcbiAgICB9XG4gICAgaWYgKGVsZW1lbnQucGFyZW50Tm9kZSA9PT0gbnVsbCkge1xuICAgICAgICByZXR1cm4gXCJOVUxML1wiICsgZWxlbWVudC50YWdOYW1lO1xuICAgIH1cbiAgICBsZXQgc2libGluZ0luZGV4ID0gMTtcbiAgICBjb25zdCBzaWJsaW5ncyA9IGVsZW1lbnQucGFyZW50Tm9kZS5jaGlsZE5vZGVzO1xuICAgIGZvciAoY29uc3Qgc2libGluZyBvZiBzaWJsaW5ncykge1xuICAgICAgICBpZiAoc2libGluZyA9PT0gZWxlbWVudCkge1xuICAgICAgICAgICAgbGV0IHBhdGggPSBnZXRQYXRoVG9Eb21FbGVtZW50KGVsZW1lbnQucGFyZW50Tm9kZSwgdmlzaWJpbGl0eUF0dHIpO1xuICAgICAgICAgICAgcGF0aCArPSBcIi9cIiArIGVsZW1lbnQudGFnTmFtZSArIFwiW1wiICsgc2libGluZ0luZGV4O1xuICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuaWQ7XG4gICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5jbGFzc05hbWU7XG4gICAgICAgICAgICBpZiAodmlzaWJpbGl0eUF0dHIpIHtcbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5oaWRkZW47XG4gICAgICAgICAgICAgICAgcGF0aCArPSBcIixcIiArIGVsZW1lbnQuc3R5bGUuZGlzcGxheTtcbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5zdHlsZS52aXNpYmlsaXR5O1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKGVsZW1lbnQudGFnTmFtZSA9PT0gXCJBXCIpIHtcbiAgICAgICAgICAgICAgICBwYXRoICs9IFwiLFwiICsgZWxlbWVudC5ocmVmO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcGF0aCArPSBcIl1cIjtcbiAgICAgICAgICAgIHJldHVybiBwYXRoO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzaWJsaW5nLm5vZGVUeXBlID09PSAxICYmIHNpYmxpbmcudGFnTmFtZSA9PT0gZWxlbWVudC50YWdOYW1lKSB7XG4gICAgICAgICAgICBzaWJsaW5nSW5kZXgrKztcbiAgICAgICAgfVxuICAgIH1cbn1cbmZ1bmN0aW9uIGdldE9yaWdpbmF0aW5nU2NyaXB0Q29udGV4dChnZXRDYWxsU3RhY2sgPSBmYWxzZSwgaXNDYWxsID0gZmFsc2UpIHtcbiAgICBjb25zdCB0cmFjZSA9ICgwLCBlcnJvcl8xLmdldFN0YWNrVHJhY2UpKCkudHJpbSgpLnNwbGl0KFwiXFxuXCIpO1xuICAgIC8vIHJldHVybiBhIGNvbnRleHQgb2JqZWN0IGV2ZW4gaWYgdGhlcmUgaXMgYW4gZXJyb3JcbiAgICBjb25zdCBlbXB0eV9jb250ZXh0ID0ge1xuICAgICAgICBzY3JpcHRVcmw6IFwiXCIsXG4gICAgICAgIHNjcmlwdExpbmU6IFwiXCIsXG4gICAgICAgIHNjcmlwdENvbDogXCJcIixcbiAgICAgICAgZnVuY05hbWU6IFwiXCIsXG4gICAgICAgIHNjcmlwdExvY0V2YWw6IFwiXCIsXG4gICAgICAgIGNhbGxTdGFjazogXCJcIixcbiAgICB9O1xuICAgIGlmICh0cmFjZS5sZW5ndGggPCA0KSB7XG4gICAgICAgIHJldHVybiBlbXB0eV9jb250ZXh0O1xuICAgIH1cbiAgICBsZXQgdHJhY2VTdGFydCA9ICgwLCBlcnJvcl8xLmdldEJlZ2luT2ZTY3JpcHRDYWxscykodHJhY2UpO1xuICAgIGlmICh0cmFjZVN0YXJ0ID09PSAtMSkge1xuICAgICAgICAvLyBJZiBub3QgaW5jbHVkZWQsIHVzZSBoZXVyaXN0aWMsIDAtMyBvciAwLTIgYXJlIE9wZW5XUE1zIGZ1bmN0aW9uc1xuICAgICAgICB0cmFjZVN0YXJ0ID0gaXNDYWxsID8gMyA6IDQ7XG4gICAgfVxuICAgIGNvbnN0IGNhbGxTaXRlID0gdHJhY2VbdHJhY2VTdGFydF07XG4gICAgaWYgKCFjYWxsU2l0ZSkge1xuICAgICAgICByZXR1cm4gZW1wdHlfY29udGV4dDtcbiAgICB9XG4gICAgLypcbiAgICAgKiBTdGFjayBmcmFtZSBmb3JtYXQgaXMgc2ltcGx5OiBGVU5DX05BTUVARklMRU5BTUU6TElORV9OTzpDT0xVTU5fTk9cbiAgICAgKlxuICAgICAqIElmIGV2YWwgb3IgRnVuY3Rpb24gaXMgaW52b2x2ZWQgd2UgaGF2ZSBhbiBhZGRpdGlvbmFsIHBhcnQgYWZ0ZXIgdGhlIEZJTEVOQU1FLCBlLmcuOlxuICAgICAqIEZVTkNfTkFNRUBGSUxFTkFNRSBsaW5lIDEyMyA+IGV2YWwgbGluZSAxID4gZXZhbDpMSU5FX05POkNPTFVNTl9OT1xuICAgICAqIG9yIEZVTkNfTkFNRUBGSUxFTkFNRSBsaW5lIDIzNCA+IEZ1bmN0aW9uOkxJTkVfTk86Q09MVU1OX05PXG4gICAgICpcbiAgICAgKiBXZSBzdG9yZSB0aGUgcGFydCBiZXR3ZWVuIHRoZSBGSUxFTkFNRSBhbmQgdGhlIExJTkVfTk8gaW4gc2NyaXB0TG9jRXZhbFxuICAgICAqL1xuICAgIHRyeSB7XG4gICAgICAgIGxldCBzY3JpcHRVcmwgPSBcIlwiO1xuICAgICAgICBsZXQgc2NyaXB0TG9jRXZhbCA9IFwiXCI7IC8vIGZvciBldmFsIG9yIEZ1bmN0aW9uIGNhbGxzXG4gICAgICAgIGNvbnN0IGNhbGxTaXRlUGFydHMgPSBjYWxsU2l0ZS5zcGxpdChcIkBcIik7XG4gICAgICAgIGNvbnN0IGZ1bmNOYW1lID0gY2FsbFNpdGVQYXJ0c1swXSB8fCBcIlwiO1xuICAgICAgICBjb25zdCBpdGVtcyA9IHJzcGxpdChjYWxsU2l0ZVBhcnRzWzFdLCBcIjpcIiwgMik7XG4gICAgICAgIGNvbnN0IGNvbHVtbk5vID0gaXRlbXNbaXRlbXMubGVuZ3RoIC0gMV07XG4gICAgICAgIGNvbnN0IGxpbmVObyA9IGl0ZW1zW2l0ZW1zLmxlbmd0aCAtIDJdO1xuICAgICAgICBjb25zdCBzY3JpcHRGaWxlTmFtZSA9IGl0ZW1zW2l0ZW1zLmxlbmd0aCAtIDNdIHx8IFwiXCI7XG4gICAgICAgIGNvbnN0IGxpbmVOb0lkeCA9IHNjcmlwdEZpbGVOYW1lLmluZGV4T2YoXCIgbGluZSBcIik7IC8vIGxpbmUgaW4gdGhlIFVSTCBtZWFucyBldmFsIG9yIEZ1bmN0aW9uXG4gICAgICAgIGlmIChsaW5lTm9JZHggPT09IC0xKSB7XG4gICAgICAgICAgICBzY3JpcHRVcmwgPSBzY3JpcHRGaWxlTmFtZTsgLy8gVE9ETzogc29tZXRpbWVzIHdlIGhhdmUgZmlsZW5hbWUgb25seSwgZS5nLiBYWC5qc1xuICAgICAgICB9XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgc2NyaXB0VXJsID0gc2NyaXB0RmlsZU5hbWUuc2xpY2UoMCwgbGluZU5vSWR4KTtcbiAgICAgICAgICAgIHNjcmlwdExvY0V2YWwgPSBzY3JpcHRGaWxlTmFtZS5zbGljZShsaW5lTm9JZHggKyAxLCBzY3JpcHRGaWxlTmFtZS5sZW5ndGgpO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IGNhbGxDb250ZXh0ID0ge1xuICAgICAgICAgICAgc2NyaXB0VXJsLFxuICAgICAgICAgICAgc2NyaXB0TGluZTogbGluZU5vLFxuICAgICAgICAgICAgc2NyaXB0Q29sOiBjb2x1bW5ObyxcbiAgICAgICAgICAgIGZ1bmNOYW1lLFxuICAgICAgICAgICAgc2NyaXB0TG9jRXZhbCxcbiAgICAgICAgICAgIGNhbGxTdGFjazogZ2V0Q2FsbFN0YWNrID8gdHJhY2Uuc2xpY2UoMykuam9pbihcIlxcblwiKS50cmltKCkgOiBcIlwiLFxuICAgICAgICB9O1xuICAgICAgICByZXR1cm4gY2FsbENvbnRleHQ7XG4gICAgfVxuICAgIGNhdGNoIChlKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogRXJyb3IgcGFyc2luZyB0aGUgc2NyaXB0IGNvbnRleHRcIiwgZS50b1N0cmluZygpLCBjYWxsU2l0ZSk7XG4gICAgICAgIHJldHVybiBlbXB0eV9jb250ZXh0O1xuICAgIH1cbn1cbi8vIGZ1bmN0aW9uIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yLCBjb250ZXh0ID0gZmFsc2UpIHtcbi8vICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3IgbmFtZTogXCIgKyBlcnJvci5uYW1lKTtcbi8vICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3IgbWVzc2FnZTogXCIgKyBlcnJvci5tZXNzYWdlKTtcbi8vICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3IgZmlsZW5hbWU6IFwiICsgZXJyb3IuZmlsZU5hbWUpO1xuLy8gICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBsaW5lIG51bWJlcjogXCIgKyBlcnJvci5saW5lTnVtYmVyKTtcbi8vICAgICBjb25zb2xlLmVycm9yKFwiT3BlbldQTTogRXJyb3Igc3RhY2s6IFwiICsgZXJyb3Iuc3RhY2spO1xuLy8gICAgIGlmIChjb250ZXh0KSB7XG4vLyAgICAgICAgIGNvbnNvbGUuZXJyb3IoXCJPcGVuV1BNOiBFcnJvciBjb250ZXh0OiBcIiArIEpTT04uc3RyaW5naWZ5KGNvbnRleHQpKTtcbi8vICAgICB9XG4vLyB9XG4vLyBGb3IgZ2V0cywgc2V0cywgZXRjLiBvbiBhIHNpbmdsZSB2YWx1ZVxuZnVuY3Rpb24gbG9nVmFsdWUoaW5zdHJ1bWVudGVkVmFyaWFibGVOYW1lLCAvLyA6IHN0cmluZyxcbnZhbHVlLCAvLyA6IGFueSxcbm9wZXJhdGlvbiwgLy8gOiBzdHJpbmcsIC8vIGZyb20gSlNPcGVyYXRpb24gb2JqZWN0IHBsZWFzZVxuY2FsbENvbnRleHQsIC8vIDogYW55LFxubG9nU2V0dGluZ3MgPSB7XG4gICAgZGVwdGg6IDAsXG4gICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXSxcbiAgICBsb2dDYWxsU3RhY2s6IGZhbHNlLFxuICAgIGxvZ0Z1bmN0aW9uR2V0czogZmFsc2UsXG4gICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICBwcmV2ZW50U2V0czogZmFsc2UsXG4gICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICBsb2dGdW5jdGlvbnNBc1N0cmluZ3M6IGZhbHNlLFxufSkge1xuICAgIGlmIChpbkxvZykge1xuICAgICAgICByZXR1cm47XG4gICAgfVxuICAgIGluTG9nID0gdHJ1ZTtcbiAgICBjb25zdCBvdmVyTGltaXQgPSB1cGRhdGVDb3VudGVyQW5kQ2hlY2tJZk92ZXIoY2FsbENvbnRleHQuc2NyaXB0VXJsLCBpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUpO1xuICAgIGlmIChvdmVyTGltaXQpIHtcbiAgICAgICAgaW5Mb2cgPSBmYWxzZTtcbiAgICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBtc2cgPSB7XG4gICAgICAgIG9wZXJhdGlvbixcbiAgICAgICAgc3ltYm9sOiBpbnN0cnVtZW50ZWRWYXJpYWJsZU5hbWUsXG4gICAgICAgIHZhbHVlOiBzZXJpYWxpemVPYmplY3QodmFsdWUsIGxvZ1NldHRpbmdzLmxvZ0Z1bmN0aW9uc0FzU3RyaW5ncyksXG4gICAgICAgIHNjcmlwdFVybDogY2FsbENvbnRleHQuc2NyaXB0VXJsLFxuICAgICAgICBzY3JpcHRMaW5lOiBjYWxsQ29udGV4dC5zY3JpcHRMaW5lLFxuICAgICAgICBzY3JpcHRDb2w6IGNhbGxDb250ZXh0LnNjcmlwdENvbCxcbiAgICAgICAgZnVuY05hbWU6IGNhbGxDb250ZXh0LmZ1bmNOYW1lLFxuICAgICAgICBzY3JpcHRMb2NFdmFsOiBjYWxsQ29udGV4dC5zY3JpcHRMb2NFdmFsLFxuICAgICAgICBjYWxsU3RhY2s6IGNhbGxDb250ZXh0LmNhbGxTdGFjayxcbiAgICAgICAgb3JkaW5hbDogb3JkaW5hbCsrLFxuICAgIH07XG4gICAgdHJ5IHtcbiAgICAgICAgbm90aWZ5KFwibG9nVmFsdWVcIiwgbXNnKTtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogVW5zdWNjZXNzZnVsIHZhbHVlIGxvZyFcIik7XG4gICAgICAgIC8vIEFjdGl2YXRlIGZvciBkZWJ1Z2dpbmcgcHVycG9zZVxuICAgICAgICAvLyBsb2dFcnJvclRvQ29uc29sZShlcnJvcik7XG4gICAgfVxuICAgIGluTG9nID0gZmFsc2U7XG59XG4vLyBGb3IgZnVuY3Rpb25zXG5mdW5jdGlvbiBsb2dDYWxsKGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSwgYXJncywgY2FsbENvbnRleHQpIHtcbiAgICBpZiAoaW5Mb2cpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBpbkxvZyA9IHRydWU7XG4gICAgY29uc3Qgb3ZlckxpbWl0ID0gdXBkYXRlQ291bnRlckFuZENoZWNrSWZPdmVyKGNhbGxDb250ZXh0LnNjcmlwdFVybCwgaW5zdHJ1bWVudGVkRnVuY3Rpb25OYW1lKTtcbiAgICBpZiAob3ZlckxpbWl0KSB7XG4gICAgICAgIGluTG9nID0gZmFsc2U7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdHJ5IHtcbiAgICAgICAgLy8gQ29udmVydCBzcGVjaWFsIGFyZ3VtZW50cyBhcnJheSB0byBhIHN0YW5kYXJkIGFycmF5IGZvciBKU09OaWZ5aW5nXG4gICAgICAgIGNvbnN0IHNlcmlhbEFyZ3MgPSBbXTtcbiAgICAgICAgZm9yIChjb25zdCBhcmcgb2YgYXJncykge1xuICAgICAgICAgICAgc2VyaWFsQXJncy5wdXNoKHNlcmlhbGl6ZU9iamVjdChhcmcsIGZhbHNlKSk7IC8vIFRPRE86IEdldCBiYWNrIHRvIGxvZ1NldHRpbmdzLmxvZ0Z1bmN0aW9uc0FzU3RyaW5ncykpO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG1zZyA9IHtcbiAgICAgICAgICAgIG9wZXJhdGlvbjogSlNPcGVyYXRpb24uY2FsbCxcbiAgICAgICAgICAgIHN5bWJvbDogaW5zdHJ1bWVudGVkRnVuY3Rpb25OYW1lLFxuICAgICAgICAgICAgYXJnczogc2VyaWFsQXJncyxcbiAgICAgICAgICAgIHZhbHVlOiBcIlwiLFxuICAgICAgICAgICAgc2NyaXB0VXJsOiBjYWxsQ29udGV4dC5zY3JpcHRVcmwsXG4gICAgICAgICAgICBzY3JpcHRMaW5lOiBjYWxsQ29udGV4dC5zY3JpcHRMaW5lLFxuICAgICAgICAgICAgc2NyaXB0Q29sOiBjYWxsQ29udGV4dC5zY3JpcHRDb2wsXG4gICAgICAgICAgICBmdW5jTmFtZTogY2FsbENvbnRleHQuZnVuY05hbWUsXG4gICAgICAgICAgICBzY3JpcHRMb2NFdmFsOiBjYWxsQ29udGV4dC5zY3JpcHRMb2NFdmFsLFxuICAgICAgICAgICAgY2FsbFN0YWNrOiBjYWxsQ29udGV4dC5jYWxsU3RhY2ssXG4gICAgICAgICAgICBvcmRpbmFsOiBvcmRpbmFsKyssXG4gICAgICAgIH07XG4gICAgICAgIG5vdGlmeShcImxvZ0NhbGxcIiwgbXNnKTtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiT3BlbldQTTogVW5zdWNjZXNzZnVsIGNhbGwgbG9nOiBcIiArIGluc3RydW1lbnRlZEZ1bmN0aW9uTmFtZSk7XG4gICAgICAgIC8vIEFjdGl2YXRlIGZvciBkZWJ1Z2dpbmcgcHVycG9zZVxuICAgICAgICAvLyBjb25zb2xlLmxvZyhlcnJvcik7XG4gICAgICAgIC8vIGxvZ0Vycm9yVG9Db25zb2xlKGVycm9yKTtcbiAgICB9XG4gICAgaW5Mb2cgPSBmYWxzZTtcbn1cbk9iamVjdC5wcm90b3R5cGUuZ2V0UHJvdG90eXBlQnlEZXB0aCA9IGZ1bmN0aW9uIChzdWJqZWN0LCBkZXB0aCkge1xuICAgIGlmIChzdWJqZWN0ID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKFwiQ2FuJ3QgZ2V0IHByb3BlcnR5IG5hbWVzIGZvciB1bmRlZmluZWRcIik7XG4gICAgfVxuICAgIGlmIChkZXB0aCA9PT0gdW5kZWZpbmVkIHx8IHR5cGVvZiBkZXB0aCAhPT0gXCJudW1iZXJcIikge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXCJEZXB0aCBcIiArIGRlcHRoICsgXCIgaXMgaW52YWxpZFwiKTtcbiAgICB9XG4gICAgbGV0IHByb3RvID0gc3ViamVjdDtcbiAgICBmb3IgKGxldCBpID0gMTsgaSA8PSBkZXB0aDsgaSsrKSB7XG4gICAgICAgIHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHByb3RvKTtcbiAgICB9XG4gICAgaWYgKHByb3RvID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKFwiUHJvdG90eXBlIHdhcyB1bmRlZmluZWQuIFRvbyBkZWVwIGl0ZXJhdGlvbj9cIik7XG4gICAgfVxuICAgIHJldHVybiBwcm90bztcbn07XG4vKipcbiAqIFRyYXZlcnNlcyB0aGUgcHJvdG90eXBlIGNoYWluIHRvIGNvbGxlY3QgcHJvcGVydGllcy4gUmV0dXJucyBhbiBhcnJheSBjb250YWluaW5nXG4gKiBhbiBvYmplY3Qgd2l0aCB0aGUgZGVwdGgsIHByb3BlcnR5TmFtZXMgYW5kIHNjYW5uZWQgc3ViamVjdFxuICovXG5PYmplY3QucHJvdG90eXBlLmdldFByb3BlcnR5TmFtZXNQZXJEZXB0aCA9IGZ1bmN0aW9uIChzdWJqZWN0LCBtYXhEZXB0aCA9IDApIHtcbiAgICBpZiAoc3ViamVjdCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcIkNhbid0IGdldCBwcm9wZXJ0eSBuYW1lcyBmb3IgdW5kZWZpbmVkXCIpO1xuICAgIH1cbiAgICBjb25zdCByZXMgPSBbXTtcbiAgICBsZXQgZGVwdGggPSAwO1xuICAgIGxldCBwcm9wZXJ0aWVzID0gT2JqZWN0LmdldE93blByb3BlcnR5TmFtZXMoc3ViamVjdCk7XG4gICAgcmVzLnB1c2goeyBkZXB0aCwgcHJvcGVydHlOYW1lczogcHJvcGVydGllcywgb2JqZWN0OiBzdWJqZWN0IH0pO1xuICAgIGxldCBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihzdWJqZWN0KTtcbiAgICB3aGlsZSAocHJvdG8gIT09IG51bGwgJiYgZGVwdGggPCBtYXhEZXB0aCkge1xuICAgICAgICBkZXB0aCsrO1xuICAgICAgICBwcm9wZXJ0aWVzID0gT2JqZWN0LmdldE93blByb3BlcnR5TmFtZXMocHJvdG8pO1xuICAgICAgICByZXMucHVzaCh7IGRlcHRoLCBwcm9wZXJ0eU5hbWVzOiBwcm9wZXJ0aWVzLCBvYmplY3Q6IHByb3RvIH0pO1xuICAgICAgICBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihwcm90byk7XG4gICAgfVxuICAgIHJldHVybiByZXM7XG59O1xuLyoqXG4gKiBGaW5kcyBhIHByb3BlcnR5IGFsb25nIHRoZSBwcm90b3R5cGUgY2hhaW5cbiAqL1xuT2JqZWN0LnByb3RvdHlwZS5maW5kUHJvcGVydHlJbkNoYWluID0gZnVuY3Rpb24gKHN1YmplY3QsIHByb3BlcnR5TmFtZSkge1xuICAgIGlmIChzdWJqZWN0ID09PSB1bmRlZmluZWQgfHwgcHJvcGVydHlOYW1lID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKFwiT2JqZWN0IGFuZCBwcm9wZXJ0eSBuYW1lIG11c3QgYmUgZGVmaW5lZFwiKTtcbiAgICB9XG4gICAgbGV0IHByb3BlcnRpZXMgPSBbXTtcbiAgICBsZXQgZGVwdGggPSAwO1xuICAgIHdoaWxlIChzdWJqZWN0ICE9PSBudWxsKSB7XG4gICAgICAgIHByb3BlcnRpZXMgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhzdWJqZWN0KTtcbiAgICAgICAgaWYgKHByb3BlcnRpZXMuaW5jbHVkZXMocHJvcGVydHlOYW1lKSkge1xuICAgICAgICAgICAgcmV0dXJuIHsgZGVwdGgsIHByb3BlcnR5TmFtZSB9O1xuICAgICAgICB9XG4gICAgICAgIGRlcHRoKys7XG4gICAgICAgIHN1YmplY3QgPSBPYmplY3QuZ2V0UHJvdG90eXBlT2Yoc3ViamVjdCk7XG4gICAgfVxuICAgIHRocm93IEVycm9yKFwiUHJvcGVydHkgbm90IGZvdW5kLiBDaGVjayB3aGV0aGVyIGNvbmZpZ3VyYXRpb24gaXMgY29ycmVjdCFcIik7XG59O1xuLypcbiAqIEdldCBhbGwga2V5cyBmb3IgcHJvcGVydGllcyB0aGF0IHNoYWxsIGJlIG92ZXJ3cml0dGVuXG4gKi9cbmZ1bmN0aW9uIGdldFByb3BlcnR5S2V5c1RvT3ZlcndyaXRlKGl0ZW0pIHtcbiAgICBjb25zdCByZXMgPSBbXTtcbiAgICBpdGVtLmxvZ1NldHRpbmdzLm92ZXJ3cml0dGVuUHJvcGVydGllcy5mb3JFYWNoKChvYmopID0+IHtcbiAgICAgICAgcmVzLnB1c2gob2JqLmtleSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIHJlcztcbn1cbmZ1bmN0aW9uIGdldENvbnRleHR1YWxQcm90b3R5cGVGcm9tU3RyaW5nKGNvbnRleHQsIG9iamVjdEFzU3RyaW5nKSB7XG4gICAgY29uc3Qgb2JqID0gY29udGV4dFtvYmplY3RBc1N0cmluZ107XG4gICAgaWYgKG9iaikge1xuICAgICAgICByZXR1cm4gb2JqLnByb3RvdHlwZSA/IG9iai5wcm90b3R5cGUgOiBPYmplY3QuZ2V0UHJvdG90eXBlT2Yob2JqKTtcbiAgICB9XG4gICAgZWxzZSB7XG4gICAgICAgIHJldHVybiB1bmRlZmluZWQ7XG4gICAgfVxufVxuLyoqXG4gKiBQcmVwYXJlcyBhIGxpc3Qgb2YgcHJvcGVydGllcyB0aGF0IG5lZWQgdG8gYmUgaW5zdHJ1bWVudGVkXG4gKiBIZXJlLCB0aGlzIGNhbiBiZSBhIHByZXZpb3VzIGNyZWF0ZWQgbGlzdCAoc2V0dGluZ3MuanM6IHByb3BlcnRpZXNUb0luc3RydW1lbnQpXG4gKiBvciBhbGwgcHJvcGVydGllcyBvZiBhIGdpdmVuIG9iamVjdCAoc2V0dGluZ3MuanM6IHByb3BlcnRpZXNUb0luc3RydW1lbnQgaXMgZW1wdHkpXG4gKi9cbmZ1bmN0aW9uIGdldE9iamVjdFByb3BlcnRpZXMoY29udGV4dCwgaXRlbSkge1xuICAgIGxldCBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50ID0gaXRlbS5sb2dTZXR0aW5ncy5wcm9wZXJ0aWVzVG9JbnN0cnVtZW50O1xuICAgIGNvbnN0IHByb3RvID0gZ2V0Q29udGV4dHVhbFByb3RvdHlwZUZyb21TdHJpbmcoY29udGV4dCwgaXRlbS5vYmplY3QpO1xuICAgIGlmICghcHJvdG8pIHtcbiAgICAgICAgdGhyb3cgRXJyb3IoXCJPYmplY3QgXCIgKyBpdGVtLm9iamVjdCArIFwiIHdhcyB1bmRlZmluZWQuXCIpO1xuICAgIH1cbiAgICBpZiAocHJvcGVydGllc1RvSW5zdHJ1bWVudCA9PT0gdW5kZWZpbmVkIHx8ICFwcm9wZXJ0aWVzVG9JbnN0cnVtZW50Lmxlbmd0aCkge1xuICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50ID0gT2JqZWN0LmdldFByb3BlcnR5TmFtZXNQZXJEZXB0aChwcm90bywgaXRlbS5kZXB0aCk7XG4gICAgICAgIC8vIGZpbHRlciBleGNsdWRlZCBhbmQgb3ZlcndyaXR0ZW4gcHJvcGVydGllc1xuICAgICAgICBjb25zdCBleGNsdWRlZCA9IGdldFByb3BlcnR5S2V5c1RvT3ZlcndyaXRlKGl0ZW0pLmNvbmNhdChpdGVtLmxvZ1NldHRpbmdzLmV4Y2x1ZGVkUHJvcGVydGllcyk7XG4gICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQgPSBmaWx0ZXJQcm9wZXJ0aWVzUGVyRGVwdGgocHJvcGVydGllc1RvSW5zdHJ1bWVudCwgZXhjbHVkZWQpO1xuICAgIH1cbiAgICBlbHNlIHtcbiAgICAgICAgLy8gaW5jbHVkZSB0aGUgb2JqZWN0IHRvIGVhY2ggaXRlbVxuICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50LmZvckVhY2goKHByb3BlcnR5TGlzdCkgPT4ge1xuICAgICAgICAgICAgcHJvcGVydHlMaXN0Lm9iamVjdCA9IE9iamVjdC5nZXRQcm90b3R5cGVCeURlcHRoKHByb3RvLCBwcm9wZXJ0eUxpc3QuZGVwdGgpO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgcmV0dXJuIHByb3BlcnRpZXNUb0luc3RydW1lbnQ7XG59XG4vKlxuICogRW5hYmxlcyBjb21tdW5pY2F0aW9uIHdpdGggYSBiYWNrZ3JvdW5kIHNjcmlwdFxuICogTXVzdCBiZSBpbmplY3RlZCBpbiBhIHByaXZhdGUgc2NvcGUgdG8gdGhlXG4gKiBwYWdlIGNvbnRleHQhXG4gKlxuICogQHBhcmFtIGRldGFpbHM6IHByb3BlcnR5IGFjY2VzcyBkZXRhaWxzXG4gKi9cbmZ1bmN0aW9uIG5vdGlmeSh0eXBlLCBjb250ZW50KSB7XG4gICAgY29udGVudC50aW1lU3RhbXAgPSBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7XG4gICAgYnJvd3Nlci5ydW50aW1lLnNlbmRNZXNzYWdlKHtcbiAgICAgICAgbmFtZXNwYWNlOiBcImphdmFzY3JpcHQtaW5zdHJ1bWVudGF0aW9uXCIsXG4gICAgICAgIHR5cGUsXG4gICAgICAgIGRhdGE6IGNvbnRlbnQsXG4gICAgfSk7XG59XG5mdW5jdGlvbiBmaWx0ZXJQcm9wZXJ0aWVzUGVyRGVwdGgoY29sbGVjdGlvbiwgZXhjbHVkZWQpIHtcbiAgICBmb3IgKGNvbnN0IGVsZW0gb2YgY29sbGVjdGlvbikge1xuICAgICAgICBlbGVtLnByb3BlcnR5TmFtZXMgPSBlbGVtLnByb3BlcnR5TmFtZXMuZmlsdGVyKChwKSA9PiAhZXhjbHVkZWQuaW5jbHVkZXMocCkpO1xuICAgIH1cbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbn1cbi8qXG4gKiBJbmplY3RzIGEgZnVuY3Rpb24gaW50byB0aGUgcGFnZSBjb250ZXh0XG4gKlxuICogQHBhcmFtIGZ1bmM6IEZ1bmN0aW9uIHRoYXQgc2hhbGwgYmUgZXhwb3J0ZWRcbiAqIEBwYXJhbSBjb250ZXh0OiB0YXJnZXQgRE9NXG4gKiBAcGFyYW0gbmFtZTogTmFtZSBvZiB0aGUgZnVuY3Rpb24gKGUuZy4sIGdldCB3aWR0aClcbiAqL1xuZnVuY3Rpb24gZXhwb3J0Q3VzdG9tRnVuY3Rpb24oZnVuYywgY29udGV4dCwgbmFtZSkge1xuICAgIGNvbnN0IHRhcmdldE9iamVjdCA9IGNvbnRleHQud3JhcHBlZEpTT2JqZWN0Lk9iamVjdC5jcmVhdGUobnVsbCk7XG4gICAgY29uc3QgZXhwb3J0ZWRUcnkgPSBleHBvcnRGdW5jdGlvbihmdW5jLCB0YXJnZXRPYmplY3QsIHtcbiAgICAgICAgYWxsb3dDcm9zc09yaWdpbkFyZ3VtZW50czogdHJ1ZSxcbiAgICAgICAgZGVmaW5lQXM6IG5hbWUsXG4gICAgfSk7XG4gICAgcmV0dXJuIGV4cG9ydGVkVHJ5O1xufVxuLypcbiAqIFRPRE86IEFkZCBkZXNjcmlwdGlvblxuICovXG5mdW5jdGlvbiBpbmplY3RGdW5jdGlvbihpbnN0cnVtZW50ZWRGdW5jdGlvbiwgZGVzY3JpcHRvciwgZnVuY3Rpb25UeXBlLCBwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUpIHtcbiAgICBjb25zdCBleHBvcnRlZEZ1bmN0aW9uID0gZXhwb3J0Q3VzdG9tRnVuY3Rpb24oaW5zdHJ1bWVudGVkRnVuY3Rpb24sIHdpbmRvdywgcHJvcGVydHlOYW1lKTtcbiAgICBjaGFuZ2VQcm9wZXJ0eShkZXNjcmlwdG9yLCBwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUsIGZ1bmN0aW9uVHlwZSwgZXhwb3J0ZWRGdW5jdGlvbik7XG59XG4vKlxuICogQWRkIG5vdGlmaWNhdGlvbnMgd2hlbiBhIHByb3BlcnR5IGlzIHJlcXVlc3RlZFxuICogVE9ETzogQnJpbmcgZXZlcnl0aGluZyB0b2dldGhlciBhdCB0aGlzIHBvaW50XG4gKlxuICogQHBhcmFtIG9yaWdpbmFsOiB0aGUgb3JpZ2luYWwgZ2V0dGVyL3NldHRlciBmdW5jdGlvblxuICogQHBhcmFtIG9iamVjdDpcbiAqIEBwYXJhbSBhcmdzOlxuICovXG5mdW5jdGlvbiBpbnN0cnVtZW50R2V0T2JqZWN0UHJvcGVydHkoaWRlbnRpZmllciwgb3JpZ2luYWwsIG5ld1ZhbHVlLCBvYmplY3QsIGFyZ3MpIHtcbiAgICBjb25zdCBvcmlnaW5hbFZhbHVlID0gb3JpZ2luYWwuY2FsbChvYmplY3QsIC4uLmFyZ3MpO1xuICAgIGNvbnN0IGNhbGxDb250ZXh0ID0gZ2V0T3JpZ2luYXRpbmdTY3JpcHRDb250ZXh0KHRydWUpO1xuICAgIGNvbnN0IHJldHVyblZhbHVlID0gbmV3VmFsdWUgIT09IHVuZGVmaW5lZCA/IG5ld1ZhbHVlIDogb3JpZ2luYWxWYWx1ZTtcbiAgICBsb2dWYWx1ZShpZGVudGlmaWVyLCByZXR1cm5WYWx1ZSwgSlNPcGVyYXRpb24uZ2V0LCBjYWxsQ29udGV4dCk7XG4gICAgcmV0dXJuIHJldHVyblZhbHVlO1xufVxuLypcbiAqIEFkZCBub3RpZmljYXRpb25zIHdoZW4gYSBwcm9wZXJ0eSBpcyBzZXRcbiAqXG4gKiBAcGFyYW0gb3JpZ2luYWw6IHRoZSBvcmlnaW5hbCBnZXR0ZXIvc2V0dGVyIGZ1bmN0aW9uXG4gKiBAcGFyYW0gb2JqZWN0OlxuICogQHBhcmFtIGFyZ3M6XG4gKi9cbmZ1bmN0aW9uIGluc3RydW1lbnRTZXRPYmplY3RQcm9wZXJ0eShpZGVudGlmaWVyLCBvcmlnaW5hbCwgbmV3VmFsdWUsIG9iamVjdCwgX2FyZ3MpIHtcbiAgICBjb25zdCBjYWxsQ29udGV4dCA9IGdldE9yaWdpbmF0aW5nU2NyaXB0Q29udGV4dCh0cnVlKTtcbiAgICBsb2dWYWx1ZShpZGVudGlmaWVyLCBuZXdWYWx1ZSwgb3JpZ2luYWwgPyBKU09wZXJhdGlvbi5zZXQgOiBKU09wZXJhdGlvbi5zZXRfZmFpbGVkLCBjYWxsQ29udGV4dCk7XG4gICAgcmV0dXJuICFvcmlnaW5hbCA/IG5ld1ZhbHVlIDogb3JpZ2luYWwuY2FsbChvYmplY3QsIG5ld1ZhbHVlKTtcbn1cbi8qXG4gKiBDcmVhdGVzIGEgZ2V0dGVyIGZ1bmN0aW9uXG4gKlxuICogQHBhcmFtIGRlc2NyaXB0b3I6IHRoZSBkZXNjcmlwdG9yIG9mIHRoZSBvcmlnaW5hbCBmdW5jdGlvblxuICogQHBhcmFtIGZ1bmNOYW1lOiBOYW1lIG9mIHByb3BlcnR5L2Z1bmN0aW9uIHRoYXQgc2hhbGwgYmUgb3ZlcndyaXR0ZW5cbiAqIEBwYXJhbSBuZXdWYWx1ZTogaW4gQ2FzZSB0aGUgdmFsdWUgc2hhbGwgYmUgY2hhbmdlZFxuICovXG5mdW5jdGlvbiBnZW5lcmF0ZUdldHRlcihpZGVudGlmaWVyLCBkZXNjcmlwdG9yLCBwcm9wZXJ0eU5hbWUsIG5ld1ZhbHVlID0gdW5kZWZpbmVkKSB7XG4gICAgY29uc3Qgb3JpZ2luYWwgPSBkZXNjcmlwdG9yLmdldDtcbiAgICByZXR1cm4gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcih7XG4gICAgICAgIGdldCBbcHJvcGVydHlOYW1lXSgpIHtcbiAgICAgICAgICAgIHJldHVybiBpbnN0cnVtZW50R2V0T2JqZWN0UHJvcGVydHkoaWRlbnRpZmllciwgb3JpZ2luYWwsIG5ld1ZhbHVlLCB0aGlzLCBhcmd1bWVudHMpO1xuICAgICAgICB9LFxuICAgIH0sIHByb3BlcnR5TmFtZSkuZ2V0O1xufVxuLypcbiAqIENyZWF0ZXMgYSBzZXR0ZXIgZnVuY3Rpb25cbiAqXG4gKiBAcGFyYW0gZGVzY3JpcHRvcjogdGhlIGRlc2NyaXB0b3Igb2YgdGhlIG9yaWdpbmFsIGZ1bmN0aW9uXG4gKiBAcGFyYW0gZnVuY05hbWU6IE5hbWUgb2YgcHJvcGVydHkvZnVuY3Rpb24gdGhhdCBzaGFsbCBiZSBvdmVyd3JpdHRlblxuICogQHBhcmFtIG5ld1ZhbHVlOiBpbiBDYXNlIHRoZSB2YWx1ZSBzaGFsbCBiZSBjaGFuZ2VkXG4gKi9cbmZ1bmN0aW9uIGdlbmVyYXRlU2V0dGVyKGlkZW50aWZpZXIsIGRlc2NyaXB0b3IsIHByb3BlcnR5TmFtZSwgX25ld1ZhbHVlID0gdW5kZWZpbmVkKSB7XG4gICAgY29uc3Qgb3JpZ2luYWwgPSBkZXNjcmlwdG9yLnNldDtcbiAgICByZXR1cm4gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcih7XG4gICAgICAgIHNldChvYmosIF9wcm9wLCB2YWx1ZSkge1xuICAgICAgICAgICAgLy8gX3Byb3AgPT09IHByb3BlcnR5TmFtZVxuICAgICAgICAgICAgcmV0dXJuIGluc3RydW1lbnRTZXRPYmplY3RQcm9wZXJ0eShpZGVudGlmaWVyLCBvcmlnaW5hbCwgdmFsdWUsIG9iaiwgYXJndW1lbnRzKTtcbiAgICAgICAgfSxcbiAgICB9LCBwcm9wZXJ0eU5hbWUpLnNldDtcbn1cbi8qXG4gKiBPdmVyd3JpdGVzIHRoZSBwcm90b3R5cGUgdG8gYWNjZXNzIGEgcHJvcGVydHlcbiAqIEBwYXJhbVxuICovXG5mdW5jdGlvbiBjaGFuZ2VQcm9wZXJ0eShkZXNjcmlwdG9yLCBwYWdlT2JqZWN0LCBuYW1lLCBtZXRob2QsIGNoYW5nZWQpIHtcbiAgICBkZXNjcmlwdG9yW21ldGhvZF0gPSBjaGFuZ2VkO1xuICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShwYWdlT2JqZWN0LCBuYW1lLCBkZXNjcmlwdG9yKTtcbn1cbi8qXG4gKiBSZXRyaWV2ZXMgYW4gb2JqZWN0IGluIGEgY29udGV4dFxuICpcbiAqIEBwYXJhbSBjb250ZXh0OiB0aGUgd2luZG93IG9iamVjdCB0aGF0IGlzIGN1cnJlbnRseSBpbnN0cnVtZW50ZWRcbiAqIEBwYXJhbSBvYmplY3Q6IHRoZSBzdWJvYmplY3QgbmVlZGVkXG4gKi9cbmZ1bmN0aW9uIGdldFBhZ2VPYmplY3RJbkNvbnRleHQoY29udGV4dCwgY29udGV4dF9vYmplY3QpIHtcbiAgICBpZiAoY29udGV4dCA9PT0gdW5kZWZpbmVkIHx8IGNvbnRleHRfb2JqZWN0ID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICByZXR1cm4gY29udGV4dFtjb250ZXh0X29iamVjdF0ucHJvdG90eXBlIHx8IGNvbnRleHRbY29udGV4dF9vYmplY3RdO1xufVxuLypcbiAqIEVudHJ5IHBvaW50IHRvIGNyZWF0ZXMgKGcvcylldHRlciBmdW5jdGlvbnMsXG4gKiBpbnN0cnVtZW50IHRoZW0gYW5kIGluamVjdCB0aGVtIHRvIHRoZSBwYWdlXG4gKiBjb250ZXh0XG4gKi9cbmZ1bmN0aW9uIGluc3RydW1lbnRHZXR0ZXJTZXR0ZXIoZGVzY3JpcHRvciwgaWRlbnRpZmllciwgcGFnZU9iamVjdCwgcHJvcGVydHlOYW1lLCBuZXdWYWx1ZSA9IHVuZGVmaW5lZCkge1xuICAgIGxldCBpbnN0cnVtZW50ZWRGdW5jdGlvbjtcbiAgICBjb25zdCBnZXRGdW5jVHlwZSA9IFwiZ2V0XCI7XG4gICAgY29uc3Qgc2V0RnVuY1R5cGUgPSBcInNldFwiO1xuICAgIGlmIChPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwoZGVzY3JpcHRvciwgZ2V0RnVuY1R5cGUpKSB7XG4gICAgICAgIGluc3RydW1lbnRlZEZ1bmN0aW9uID0gZ2VuZXJhdGVHZXR0ZXIoaWRlbnRpZmllciwgZGVzY3JpcHRvciwgcHJvcGVydHlOYW1lLCBuZXdWYWx1ZSk7XG4gICAgICAgIGluamVjdEZ1bmN0aW9uKGluc3RydW1lbnRlZEZ1bmN0aW9uLCBkZXNjcmlwdG9yLCBnZXRGdW5jVHlwZSwgcGFnZU9iamVjdCwgcHJvcGVydHlOYW1lKTtcbiAgICB9XG4gICAgaWYgKE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbChkZXNjcmlwdG9yLCBzZXRGdW5jVHlwZSkpIHtcbiAgICAgICAgaW5zdHJ1bWVudGVkRnVuY3Rpb24gPSBnZW5lcmF0ZVNldHRlcihpZGVudGlmaWVyLCBkZXNjcmlwdG9yLCBwcm9wZXJ0eU5hbWUpO1xuICAgICAgICBpbmplY3RGdW5jdGlvbihpbnN0cnVtZW50ZWRGdW5jdGlvbiwgZGVzY3JpcHRvciwgc2V0RnVuY1R5cGUsIHBhZ2VPYmplY3QsIHByb3BlcnR5TmFtZSk7XG4gICAgfVxufVxuLypcbiAqIFRPRE86IEFkZCBkZXNjcmlwdGlvblxuICovXG5mdW5jdGlvbiBmdW5jdGlvbkdlbmVyYXRvcihfY29udGV4dCwgaWRlbnRpZmllciwgb3JpZ2luYWwsIF9mdW5jTmFtZSkge1xuICAgIGZ1bmN0aW9uIHRlbXAoKSB7XG4gICAgICAgIGxldCByZXN1bHQ7XG4gICAgICAgIGNvbnN0IGNhbGxDb250ZXh0ID0gZ2V0T3JpZ2luYXRpbmdTY3JpcHRDb250ZXh0KHRydWUsIHRydWUpO1xuICAgICAgICBsb2dDYWxsKGlkZW50aWZpZXIsIGFyZ3VtZW50cywgY2FsbENvbnRleHQpO1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgcmVzdWx0ID1cbiAgICAgICAgICAgICAgICBhcmd1bWVudHMubGVuZ3RoID4gMFxuICAgICAgICAgICAgICAgICAgICA/IG9yaWdpbmFsLmNhbGwodGhpcywgLi4uYXJndW1lbnRzKVxuICAgICAgICAgICAgICAgICAgICA6IG9yaWdpbmFsLmNhbGwodGhpcyk7XG4gICAgICAgIH1cbiAgICAgICAgY2F0Y2ggKGVycikge1xuICAgICAgICAgICAgY29uc3QgZmFrZUVycm9yID0gKDAsIGVycm9yXzEuZ2VuZXJhdGVFcnJvck9iamVjdCkoZXJyKTtcbiAgICAgICAgICAgIHRocm93IGZha2VFcnJvcjtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gcmVzdWx0O1xuICAgIH1cbiAgICByZXR1cm4gdGVtcDtcbn1cbi8qXG4gKiBUT0RPOiBBZGQgZGVzY3JpcHRpb25cbiAqL1xuZnVuY3Rpb24gaW5zdHJ1bWVudEZ1bmN0aW9uKGNvbnRleHQsIGRlc2NyaXB0b3IsIGlkZW50aWZpZXIsIHBhZ2VPYmplY3QsIHByb3BlcnR5TmFtZSkge1xuICAgIGNvbnN0IG9yaWdpbmFsID0gZGVzY3JpcHRvci52YWx1ZTtcbiAgICBjb25zdCB0ZW1wRnVuY3Rpb24gPSBmdW5jdGlvbkdlbmVyYXRvcihjb250ZXh0LCBpZGVudGlmaWVyLCBvcmlnaW5hbCwgcHJvcGVydHlOYW1lKTtcbiAgICBjb25zdCBleHBvcnRlZEZ1bmN0aW9uID0gZXhwb3J0Q3VzdG9tRnVuY3Rpb24odGVtcEZ1bmN0aW9uLCBjb250ZXh0LCBvcmlnaW5hbC5uYW1lKTtcbiAgICBjaGFuZ2VQcm9wZXJ0eShkZXNjcmlwdG9yLCBwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUsIFwidmFsdWVcIiwgZXhwb3J0ZWRGdW5jdGlvbik7XG59XG4vKlxuICogSGVscGVyIGNsYXNzIHRvIHBlcmZvcm0gYWxsIG5lZWRlZCBmdW5jdGlvbmFsaXR5XG4gKlxuICogQHBhcmFtIGNvbnRleHQ6IHRoZSB3aW5kb3cgb2JqZWN0IHRoYXQgaXMgY3VycmVudGx5IGluc3RydW1lbnRlZFxuICogQHBhcmFtIG9iamVjdDogY2hpbGQgb2JqZWN0IHRoYXQgc2hhbGwgYmUgaW5zdHVtZW50ZWRcbiAqL1xuZnVuY3Rpb24gaW5zdHJ1bWVudChjb250ZXh0LCBpdGVtLCBkZXB0aCwgcHJvcGVydHlOYW1lLCBuZXdWYWx1ZSA9IHVuZGVmaW5lZCkge1xuICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGlkZW50aWZpZXIgPSBpdGVtLmluc3RydW1lbnRlZE5hbWUgKyBcIi5cIiArIHByb3BlcnR5TmFtZTtcbiAgICAgICAgY29uc3QgaW5pdGlhbFBhZ2VPYmplY3QgPSBnZXRQYWdlT2JqZWN0SW5Db250ZXh0KGNvbnRleHQud3JhcHBlZEpTT2JqZWN0LCBpdGVtLm9iamVjdCk7XG4gICAgICAgIGNvbnN0IHBhZ2VPYmplY3QgPSBPYmplY3QuZ2V0UHJvdG90eXBlQnlEZXB0aChpbml0aWFsUGFnZU9iamVjdCwgZGVwdGgpO1xuICAgICAgICBjb25zdCBkZXNjcmlwdG9yID0gT2JqZWN0LmdldFByb3BlcnR5RGVzY3JpcHRvcihwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUpO1xuICAgICAgICBpZiAoZGVzY3JpcHRvciA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICAvLyBEbyBub3QgZG8gdW5kZWZpbmVkIGRlc2NyaXB0b3IuIFdlIGNhbiBzYWZlbHkgc2tpcCB0aGVtXG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHR5cGVvZiBkZXNjcmlwdG9yLnZhbHVlID09PSBcImZ1bmN0aW9uXCIpIHtcbiAgICAgICAgICAgIGluc3RydW1lbnRGdW5jdGlvbihjb250ZXh0LCBkZXNjcmlwdG9yLCBpZGVudGlmaWVyLCBwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUpO1xuICAgICAgICB9XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgICAgaW5zdHJ1bWVudEdldHRlclNldHRlcihkZXNjcmlwdG9yLCBpZGVudGlmaWVyLCBwYWdlT2JqZWN0LCBwcm9wZXJ0eU5hbWUsIG5ld1ZhbHVlKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBjYXRjaCAoZXJyb3IpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcihlcnJvcik7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoZXJyb3Iuc3RhY2spO1xuICAgICAgICByZXR1cm47XG4gICAgfVxufVxuLypcbiAqIENoZWNrcyBpZiBhbiBvYmplY3Qgd2FzIGFscmVhZHkgd3JhcHBlZFxuICogVW53cmFwcGVkIG9iamVjdHMgc2hvdWxkIGJlIHdyYXBwZWQgaW1tZWRpYXRlbHlcbiAqL1xuY29uc3Qgd3JhcHBlZE9iamVjdHMgPSBbXTtcbmZ1bmN0aW9uIG5lZWRzV3JhcHBlcihvYmplY3QpIHtcbiAgICBpZiAod3JhcHBlZE9iamVjdHMuc29tZSgob2JqKSA9PiBvYmplY3QgPT09IG9iaikpIHtcbiAgICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICB3cmFwcGVkT2JqZWN0cy5wdXNoKG9iamVjdCk7XG4gICAgcmV0dXJuIHRydWU7XG59XG5mdW5jdGlvbiBzdGFydEluc3RydW1lbnQoY29udGV4dCkge1xuICAgIGZvciAoY29uc3QgaXRlbSBvZiBzZXR0aW5nc18xLmpzSW5zdHJ1bWVudGF0aW9uU2V0dGluZ3MpIHtcbiAgICAgICAgLy8gcmV0cmlldmUgT2JqZWN0IHByb3BlcnRpZXMgYWxvbnQgdGhlIGNoYWluXG4gICAgICAgIGxldCBwcm9wZXJ0eUNvbGxlY3Rpb247XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICBwcm9wZXJ0eUNvbGxlY3Rpb24gPSBnZXRPYmplY3RQcm9wZXJ0aWVzKGNvbnRleHQsIGl0ZW0pO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnIpIHtcbiAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoZXJyKTtcbiAgICAgICAgICAgIGNvbnRpbnVlO1xuICAgICAgICB9XG4gICAgICAgIC8vIEluc3RydW1lbnQgZWFjaCBQcm9wZXJ0eSBwZXIgb2JqZWN0L3Byb3RvdHlwZVxuICAgICAgICBpZiAocHJvcGVydHlDb2xsZWN0aW9uWzBdICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBwcm9wZXJ0eUNvbGxlY3Rpb24uZm9yRWFjaCgoeyBkZXB0aCwgcHJvcGVydHlOYW1lcywgb2JqZWN0IH0pID0+IHtcbiAgICAgICAgICAgICAgICBpZiAobmVlZHNXcmFwcGVyKG9iamVjdCkpIHtcbiAgICAgICAgICAgICAgICAgICAgcHJvcGVydHlOYW1lcy5mb3JFYWNoKChwcm9wZXJ0eU5hbWUpID0+IGluc3RydW1lbnQoY29udGV4dCwgaXRlbSwgZGVwdGgsIHByb3BlcnR5TmFtZSkpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIC8vIEluc3RydW1lbnQgcHJvcGVydGllcyBhbmQgb3ZlcndyaXRlIHRoZWlyIHJldHVybiB2YWx1ZVxuICAgICAgICBpZiAoaXRlbS5sb2dTZXR0aW5ncy5vdmVyd3JpdHRlblByb3BlcnRpZXMpIHtcbiAgICAgICAgICAgIGl0ZW0ubG9nU2V0dGluZ3Mub3ZlcndyaXR0ZW5Qcm9wZXJ0aWVzLmZvckVhY2goKHsga2V5OiBuYW1lLCB2YWx1ZSB9KSA9PiB7XG4gICAgICAgICAgICAgICAgY29uc3QgcHJvdG8gPSBnZXRDb250ZXh0dWFsUHJvdG90eXBlRnJvbVN0cmluZyhjb250ZXh0LCBpdGVtLm9iamVjdCk7XG4gICAgICAgICAgICAgICAgaWYgKHByb3RvKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IHsgZGVwdGgsIHByb3BlcnR5TmFtZSB9ID0gT2JqZWN0LmZpbmRQcm9wZXJ0eUluQ2hhaW4ocHJvdG8sIG5hbWUpO1xuICAgICAgICAgICAgICAgICAgICBpbnN0cnVtZW50KGNvbnRleHQsIGl0ZW0sIGRlcHRoLCBwcm9wZXJ0eU5hbWUsIHZhbHVlKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoXCJDb3VsZCBub3QgaW5zdHJ1bWVudCBcIiArXG4gICAgICAgICAgICAgICAgICAgICAgICBpdGVtLm9iamVjdCArXG4gICAgICAgICAgICAgICAgICAgICAgICBcIi4gRW5jb3VudGVyZWQgdW5kZWZpbmVkIG9iamVjdC5cIik7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICB9XG59XG4vLyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LGV5SjJaWEp6YVc5dUlqb3pMQ0ptYVd4bElqb2lhVzV6ZEhKMWJXVnVkQzVxY3lJc0luTnZkWEpqWlZKdmIzUWlPaUlpTENKemIzVnlZMlZ6SWpwYklpNHVMeTR1THk0dUwzTnlZeTl6ZEdWaGJIUm9MMmx1YzNSeWRXMWxiblF1ZEhNaVhTd2libUZ0WlhNaU9sdGRMQ0p0WVhCd2FXNW5jeUk2SWpzN1FVRTRNVUpUTERCRFFVRmxPMEZCUVVVc2IwUkJRVzlDTzBGQk9URkNPVU1zZVVOQlFYVkVPMEZCUTNaRUxHMURRVWxwUWp0QlFVZHFRanM3ZVVOQlJYbERPMEZCUTNwRExHMUZRVUZ0UlR0QlFVTnVSU3hOUVVGTkxGZEJRVmNzUjBGQlJ5eEhRVUZITEVOQlFVTTdRVUZEZUVJc1lVRkJZVHRCUVVOaUxFMUJRVTBzVlVGQlZTeEhRVUZITEVWQlFVVXNRMEZCUXp0QlFVTjBRaXdyUTBGQkswTTdRVUZETDBNc1NVRkJTU3hMUVVGTExFZEJRVmtzUzBGQlN5eERRVUZETzBGQlF6TkNMR2RFUVVGblJEdEJRVU5vUkN4SlFVRkpMRTlCUVU4c1IwRkJSeXhEUVVGRExFTkJRVU03UVVGRmFFSXNNRUpCUVRCQ08wRkJRekZDTEUxQlFVMHNWMEZCVnl4SFFVRkhPMGxCUTJ4Q0xFbEJRVWtzUlVGQlJTeE5RVUZOTzBsQlExb3NSMEZCUnl4RlFVRkZMRXRCUVVzN1NVRkRWaXhWUVVGVkxFVkJRVVVzWVVGQllUdEpRVU42UWl4WlFVRlpMRVZCUVVVc1pVRkJaVHRKUVVNM1FpeEhRVUZITEVWQlFVVXNTMEZCU3p0SlFVTldMRlZCUVZVc1JVRkJSU3hoUVVGaE8wbEJRM3BDTEdGQlFXRXNSVUZCUlN4blFrRkJaMEk3UTBGRGFFTXNRMEZCUXp0QlFVVkdMREJEUVVFd1F6dEJRVU14UXl4VFFVRlRMRTFCUVUwc1EwRkJReXhOUVVGakxFVkJRVVVzUjBGQlZ5eEZRVUZGTEZGQlFXZENPMGxCUXpORUxFMUJRVTBzUzBGQlN5eEhRVUZITEUxQlFVMHNRMEZCUXl4TFFVRkxMRU5CUVVNc1IwRkJSeXhEUVVGRExFTkJRVU03U1VGRGFFTXNUMEZCVHl4UlFVRlJPMUZCUTJJc1EwRkJReXhEUVVGRExFTkJRVU1zUzBGQlN5eERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRMRVZCUVVVc1EwRkJReXhSUVVGUkxFTkJRVU1zUTBGQlF5eEpRVUZKTEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNc1EwRkJReXhOUVVGTkxFTkJRVU1zUzBGQlN5eERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRMRkZCUVZFc1EwRkJReXhEUVVGRE8xRkJRM1JGTEVOQlFVTXNRMEZCUXl4TFFVRkxMRU5CUVVNN1FVRkRXaXhEUVVGRE8wRkJSVVFzWjBOQlFXZERPMEZCUTJoRExGTkJRVk1zWlVGQlpTeERRVU4wUWl4TlFVRk5PMEZCUTA0c2RVTkJRWFZETzBGQlEzWkRMR3RDUVVFeVFqdEpRVWN6UWl3MFFrRkJORUk3U1VGRE5VSXNTVUZCU1N4RFFVRkRPMUZCUTBnc1NVRkJTU3hOUVVGTkxFdEJRVXNzU1VGQlNTeEZRVUZGTEVOQlFVTTdXVUZEY0VJc1QwRkJUeXhOUVVGTkxFTkJRVU03VVVGRGFFSXNRMEZCUXp0UlFVTkVMRWxCUVVrc1QwRkJUeXhOUVVGTkxFdEJRVXNzVlVGQlZTeEZRVUZGTEVOQlFVTTdXVUZEYWtNc1QwRkJUeXhyUWtGQmEwSXNRMEZCUXl4RFFVRkRMRU5CUVVNc1RVRkJUU3hEUVVGRExGRkJRVkVzUlVGQlJTeERRVUZETEVOQlFVTXNRMEZCUXl4VlFVRlZMRU5CUVVNN1VVRkROMFFzUTBGQlF6dFJRVU5FTEVsQlFVa3NUMEZCVHl4TlFVRk5MRXRCUVVzc1VVRkJVU3hGUVVGRkxFTkJRVU03V1VGREwwSXNUMEZCVHl4TlFVRk5MRU5CUVVNN1VVRkRhRUlzUTBGQlF6dFJRVU5FTEUxQlFVMHNWMEZCVnl4SFFVRkhMRVZCUVVVc1EwRkJRenRSUVVOMlFpeFBRVUZQTEVsQlFVa3NRMEZCUXl4VFFVRlRMRU5CUVVNc1RVRkJUU3hGUVVGRkxGVkJRVlVzUjBGQlJ5eEZRVUZGTEV0QlFVczdXVUZEYUVRc1NVRkJTU3hMUVVGTExFdEJRVXNzU1VGQlNTeEZRVUZGTEVOQlFVTTdaMEpCUTI1Q0xFOUJRVThzVFVGQlRTeERRVUZETzFsQlEyaENMRU5CUVVNN1dVRkRSQ3hKUVVGSkxFOUJRVThzUzBGQlN5eExRVUZMTEZWQlFWVXNSVUZCUlN4RFFVRkRPMmRDUVVOb1F5eFBRVUZQTEd0Q1FVRnJRaXhEUVVGRExFTkJRVU1zUTBGQlF5eExRVUZMTEVOQlFVTXNVVUZCVVN4RlFVRkZMRU5CUVVNc1EwRkJReXhEUVVGRExGVkJRVlVzUTBGQlF6dFpRVU0xUkN4RFFVRkRPMWxCUTBRc1NVRkJTU3hQUVVGUExFdEJRVXNzUzBGQlN5eFJRVUZSTEVWQlFVVXNRMEZCUXp0blFrRkRPVUlzY1VOQlFYRkRPMmRDUVVOeVF5eEpRVUZKTEdsQ1FVRnBRaXhKUVVGSkxFdEJRVXNzUlVGQlJTeERRVUZETzI5Q1FVTXZRaXhMUVVGTExFZEJRVWNzUzBGQlN5eERRVUZETEdWQlFXVXNRMEZCUXp0blFrRkRhRU1zUTBGQlF6dG5Ra0ZGUkN4NVFrRkJlVUk3WjBKQlEzcENMRWxCUVVrc1MwRkJTeXhaUVVGWkxGZEJRVmNzUlVGQlJTeERRVUZETzI5Q1FVTnFReXhQUVVGUExHMUNRVUZ0UWl4RFFVRkRMRXRCUVVzc1EwRkJReXhEUVVGRE8yZENRVU53UXl4RFFVRkRPMmRDUVVWRUxDdENRVUVyUWp0blFrRkRMMElzU1VGQlNTeEhRVUZITEV0QlFVc3NSVUZCUlN4SlFVRkpMRmRCUVZjc1EwRkJReXhQUVVGUExFTkJRVU1zUzBGQlN5eERRVUZETEVkQlFVY3NRMEZCUXl4RlFVRkZMRU5CUVVNN2IwSkJRMnBFTEZkQlFWY3NRMEZCUXl4SlFVRkpMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03YjBKQlEzaENMRTlCUVU4c1MwRkJTeXhEUVVGRE8yZENRVU5tTEVOQlFVTTdjVUpCUVUwc1EwRkJRenR2UWtGRFRpeFBRVUZQTEU5QlFVOHNTMEZCU3l4RFFVRkRPMmRDUVVOMFFpeERRVUZETzFsQlEwZ3NRMEZCUXp0WlFVTkVMRTlCUVU4c1MwRkJTeXhEUVVGRE8xRkJRMllzUTBGQlF5eERRVUZETEVOQlFVTTdTVUZEVEN4RFFVRkRPMGxCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVVzUTBGQlF6dFJRVU5tTEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc1owTkJRV2RETEVkQlFVY3NTMEZCU3l4RFFVRkRMRU5CUVVNN1VVRkRkRVFzVDBGQlR5eDFRa0ZCZFVJc1IwRkJSeXhMUVVGTExFTkJRVU03U1VGRGVrTXNRMEZCUXp0QlFVTklMRU5CUVVNN1FVRkZSQ3h2UmtGQmIwWTdRVUZEY0VZc2VVVkJRWGxGTzBGQlEzcEZMRTFCUVUwc1EwRkJReXh4UWtGQmNVSXNSMEZCUnl4VlFVRlZMRTlCUVU4c1JVRkJSU3hKUVVGSk8wbEJRM0JFTEVsQlFVa3NUMEZCVHl4TFFVRkxMRk5CUVZNc1JVRkJSU3hEUVVGRE8xRkJRekZDTEUxQlFVMHNTVUZCU1N4TFFVRkxMRU5CUVVNc05rTkJRVFpETEVOQlFVTXNRMEZCUXp0SlFVTnFSU3hEUVVGRE8wbEJRMFFzU1VGQlNTeEZRVUZGTEVkQlFVY3NUVUZCVFN4RFFVRkRMSGRDUVVGM1FpeERRVUZETEU5QlFVOHNSVUZCUlN4SlFVRkpMRU5CUVVNc1EwRkJRenRKUVVONFJDeEpRVUZKTEV0QlFVc3NSMEZCUnl4TlFVRk5MRU5CUVVNc1kwRkJZeXhEUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzBsQlF6TkRMRTlCUVU4c1JVRkJSU3hMUVVGTExGTkJRVk1zU1VGQlNTeExRVUZMTEV0QlFVc3NTVUZCU1N4RlFVRkZMRU5CUVVNN1VVRkRNVU1zUlVGQlJTeEhRVUZITEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4RFFVRkRMRU5CUVVNN1VVRkRiRVFzUzBGQlN5eEhRVUZITEUxQlFVMHNRMEZCUXl4alFVRmpMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03U1VGRGRrTXNRMEZCUXp0SlFVTkVMRTlCUVU4c1JVRkJSU3hEUVVGRE8wRkJRMW9zUTBGQlF5eERRVUZETzBGQlJVWXNVMEZCVXl3eVFrRkJNa0lzUTBGQlF5eFRRVUZUTEVWQlFVVXNUVUZCVFR0SlFVTndSQ3hOUVVGTkxFZEJRVWNzUjBGQlJ5eFRRVUZUTEVkQlFVY3NSMEZCUnl4SFFVRkhMRTFCUVUwc1EwRkJRenRKUVVOeVF5eEpRVUZKTEVkQlFVY3NTVUZCU1N4VlFVRlZMRWxCUVVrc1ZVRkJWU3hEUVVGRExFZEJRVWNzUTBGQlF5eEpRVUZKTEZkQlFWY3NSVUZCUlN4RFFVRkRPMUZCUTNoRUxFOUJRVThzU1VGQlNTeERRVUZETzBsQlEyUXNRMEZCUXp0VFFVRk5MRWxCUVVrc1EwRkJReXhEUVVGRExFZEJRVWNzU1VGQlNTeFZRVUZWTEVOQlFVTXNSVUZCUlN4RFFVRkRPMUZCUTJoRExGVkJRVlVzUTBGQlF5eEhRVUZITEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNN1NVRkRkRUlzUTBGQlF6dFRRVUZOTEVOQlFVTTdVVUZEVGl4VlFVRlZMRU5CUVVNc1IwRkJSeXhEUVVGRExFbEJRVWtzUTBGQlF5eERRVUZETzBsQlEzWkNMRU5CUVVNN1NVRkRSQ3hQUVVGUExFdEJRVXNzUTBGQlF6dEJRVU5tTEVOQlFVTTdRVUZGUkN3NFEwRkJPRU03UVVGRE9VTXNVMEZCVXl4dFFrRkJiVUlzUTBGQlF5eFBRVUZQTEVWQlFVVXNhVUpCUVRCQ0xFdEJRVXM3U1VGRGJrVXNTVUZCU1N4UFFVRlBMRXRCUVVzc1VVRkJVU3hEUVVGRExFbEJRVWtzUlVGQlJTeERRVUZETzFGQlF6bENMRTlCUVU4c1QwRkJUeXhEUVVGRExFOUJRVThzUTBGQlF6dEpRVU42UWl4RFFVRkRPMGxCUTBRc1NVRkJTU3hQUVVGUExFTkJRVU1zVlVGQlZTeExRVUZMTEVsQlFVa3NSVUZCUlN4RFFVRkRPMUZCUTJoRExFOUJRVThzVDBGQlR5eEhRVUZITEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNN1NVRkRia01zUTBGQlF6dEpRVVZFTEVsQlFVa3NXVUZCV1N4SFFVRkhMRU5CUVVNc1EwRkJRenRKUVVOeVFpeE5RVUZOTEZGQlFWRXNSMEZCUnl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hEUVVGRExGVkJRVlVzUTBGQlF6dEpRVU12UXl4TFFVRkxMRTFCUVUwc1QwRkJUeXhKUVVGSkxGRkJRVkVzUlVGQlJTeERRVUZETzFGQlF5OUNMRWxCUVVrc1QwRkJUeXhMUVVGTExFOUJRVThzUlVGQlJTeERRVUZETzFsQlEzaENMRWxCUVVrc1NVRkJTU3hIUVVGSExHMUNRVUZ0UWl4RFFVRkRMRTlCUVU4c1EwRkJReXhWUVVGVkxFVkJRVVVzWTBGQll5eERRVUZETEVOQlFVTTdXVUZEYmtVc1NVRkJTU3hKUVVGSkxFZEJRVWNzUjBGQlJ5eFBRVUZQTEVOQlFVTXNUMEZCVHl4SFFVRkhMRWRCUVVjc1IwRkJSeXhaUVVGWkxFTkJRVU03V1VGRGJrUXNTVUZCU1N4SlFVRkpMRWRCUVVjc1IwRkJSeXhQUVVGUExFTkJRVU1zUlVGQlJTeERRVUZETzFsQlEzcENMRWxCUVVrc1NVRkJTU3hIUVVGSExFZEJRVWNzVDBGQlR5eERRVUZETEZOQlFWTXNRMEZCUXp0WlFVTm9ReXhKUVVGSkxHTkJRV01zUlVGQlJTeERRVUZETzJkQ1FVTnVRaXhKUVVGSkxFbEJRVWtzUjBGQlJ5eEhRVUZITEU5QlFVOHNRMEZCUXl4TlFVRk5MRU5CUVVNN1owSkJRemRDTEVsQlFVa3NTVUZCU1N4SFFVRkhMRWRCUVVjc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eFBRVUZQTEVOQlFVTTdaMEpCUTNCRExFbEJRVWtzU1VGQlNTeEhRVUZITEVkQlFVY3NUMEZCVHl4RFFVRkRMRXRCUVVzc1EwRkJReXhWUVVGVkxFTkJRVU03V1VGRGVrTXNRMEZCUXp0WlFVTkVMRWxCUVVrc1QwRkJUeXhEUVVGRExFOUJRVThzUzBGQlN5eEhRVUZITEVWQlFVVXNRMEZCUXp0blFrRkROVUlzU1VGQlNTeEpRVUZKTEVkQlFVY3NSMEZCUnl4UFFVRlBMRU5CUVVNc1NVRkJTU3hEUVVGRE8xbEJRemRDTEVOQlFVTTdXVUZEUkN4SlFVRkpMRWxCUVVrc1IwRkJSeXhEUVVGRE8xbEJRMW9zVDBGQlR5eEpRVUZKTEVOQlFVTTdVVUZEWkN4RFFVRkRPMUZCUTBRc1NVRkJTU3hQUVVGUExFTkJRVU1zVVVGQlVTeExRVUZMTEVOQlFVTXNTVUZCU1N4UFFVRlBMRU5CUVVNc1QwRkJUeXhMUVVGTExFOUJRVThzUTBGQlF5eFBRVUZQTEVWQlFVVXNRMEZCUXp0WlFVTnNSU3haUVVGWkxFVkJRVVVzUTBGQlF6dFJRVU5xUWl4RFFVRkRPMGxCUTBnc1EwRkJRenRCUVVOSUxFTkJRVU03UVVGRlJDeFRRVUZUTERKQ1FVRXlRaXhEUVVGRExGbEJRVmtzUjBGQlJ5eExRVUZMTEVWQlFVVXNUVUZCVFN4SFFVRkhMRXRCUVVzN1NVRkRka1VzVFVGQlRTeExRVUZMTEVkQlFVY3NTVUZCUVN4eFFrRkJZU3hIUVVGRkxFTkJRVU1zU1VGQlNTeEZRVUZGTEVOQlFVTXNTMEZCU3l4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRE8wbEJRMnBFTEc5RVFVRnZSRHRKUVVOd1JDeE5RVUZOTEdGQlFXRXNSMEZCUnp0UlFVTndRaXhUUVVGVExFVkJRVVVzUlVGQlJUdFJRVU5pTEZWQlFWVXNSVUZCUlN4RlFVRkZPMUZCUTJRc1UwRkJVeXhGUVVGRkxFVkJRVVU3VVVGRFlpeFJRVUZSTEVWQlFVVXNSVUZCUlR0UlFVTmFMR0ZCUVdFc1JVRkJSU3hGUVVGRk8xRkJRMnBDTEZOQlFWTXNSVUZCUlN4RlFVRkZPMHRCUTJRc1EwRkJRenRKUVVOR0xFbEJRVWtzUzBGQlN5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRVZCUVVVc1EwRkJRenRSUVVOeVFpeFBRVUZQTEdGQlFXRXNRMEZCUXp0SlFVTjJRaXhEUVVGRE8wbEJSVVFzU1VGQlNTeFZRVUZWTEVkQlFVY3NTVUZCUVN3MlFrRkJjVUlzUlVGQlF5eExRVUZMTEVOQlFVTXNRMEZCUXp0SlFVTTVReXhKUVVGSkxGVkJRVlVzUzBGQlN5eERRVUZETEVOQlFVTXNSVUZCUlN4RFFVRkRPMUZCUTNSQ0xHOUZRVUZ2UlR0UlFVTndSU3hWUVVGVkxFZEJRVWNzVFVGQlRTeERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF6dEpRVU01UWl4RFFVRkRPMGxCUTBRc1RVRkJUU3hSUVVGUkxFZEJRV3RDTEV0QlFVc3NRMEZCUXl4VlFVRlZMRU5CUVVNc1EwRkJRenRKUVVOc1JDeEpRVUZKTEVOQlFVTXNVVUZCVVN4RlFVRkZMRU5CUVVNN1VVRkRaQ3hQUVVGUExHRkJRV0VzUTBGQlF6dEpRVU4yUWl4RFFVRkRPMGxCUTBRN096czdPenM3TzA5QlVVYzdTVUZEU0N4SlFVRkpMRU5CUVVNN1VVRkRTQ3hKUVVGSkxGTkJRVk1zUjBGQlJ5eEZRVUZGTEVOQlFVTTdVVUZEYmtJc1NVRkJTU3hoUVVGaExFZEJRVWNzUlVGQlJTeERRVUZETEVOQlFVTXNOa0pCUVRaQ08xRkJRM0pFTEUxQlFVMHNZVUZCWVN4SFFVRkhMRkZCUVZFc1EwRkJReXhMUVVGTExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTTdVVUZETVVNc1RVRkJUU3hSUVVGUkxFZEJRVWNzWVVGQllTeERRVUZETEVOQlFVTXNRMEZCUXl4SlFVRkpMRVZCUVVVc1EwRkJRenRSUVVONFF5eE5RVUZOTEV0QlFVc3NSMEZCUnl4TlFVRk5MRU5CUVVNc1lVRkJZU3hEUVVGRExFTkJRVU1zUTBGQlF5eEZRVUZGTEVkQlFVY3NSVUZCUlN4RFFVRkRMRU5CUVVNc1EwRkJRenRSUVVNdlF5eE5RVUZOTEZGQlFWRXNSMEZCUnl4TFFVRkxMRU5CUVVNc1MwRkJTeXhEUVVGRExFMUJRVTBzUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXp0UlFVTjZReXhOUVVGTkxFMUJRVTBzUjBGQlJ5eExRVUZMTEVOQlFVTXNTMEZCU3l4RFFVRkRMRTFCUVUwc1IwRkJSeXhEUVVGRExFTkJRVU1zUTBGQlF6dFJRVU4yUXl4TlFVRk5MR05CUVdNc1IwRkJSeXhMUVVGTExFTkJRVU1zUzBGQlN5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRMRU5CUVVNc1NVRkJTU3hGUVVGRkxFTkJRVU03VVVGRGNrUXNUVUZCVFN4VFFVRlRMRWRCUVVjc1kwRkJZeXhEUVVGRExFOUJRVThzUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXl4RFFVRkRMSGxEUVVGNVF6dFJRVU0zUml4SlFVRkpMRk5CUVZNc1MwRkJTeXhEUVVGRExFTkJRVU1zUlVGQlJTeERRVUZETzFsQlEzSkNMRk5CUVZNc1IwRkJSeXhqUVVGakxFTkJRVU1zUTBGQlF5eHZSRUZCYjBRN1VVRkRiRVlzUTBGQlF6dGhRVUZOTEVOQlFVTTdXVUZEVGl4VFFVRlRMRWRCUVVjc1kwRkJZeXhEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETEVWQlFVVXNVMEZCVXl4RFFVRkRMRU5CUVVNN1dVRkRMME1zWVVGQllTeEhRVUZITEdOQlFXTXNRMEZCUXl4TFFVRkxMRU5CUTJ4RExGTkJRVk1zUjBGQlJ5eERRVUZETEVWQlEySXNZMEZCWXl4RFFVRkRMRTFCUVUwc1EwRkRkRUlzUTBGQlF6dFJRVU5LTEVOQlFVTTdVVUZEUkN4TlFVRk5MRmRCUVZjc1IwRkJSenRaUVVOc1FpeFRRVUZUTzFsQlExUXNWVUZCVlN4RlFVRkZMRTFCUVUwN1dVRkRiRUlzVTBGQlV5eEZRVUZGTEZGQlFWRTdXVUZEYmtJc1VVRkJVVHRaUVVOU0xHRkJRV0U3V1VGRFlpeFRRVUZUTEVWQlFVVXNXVUZCV1N4RFFVRkRMRU5CUVVNc1EwRkJReXhMUVVGTExFTkJRVU1zUzBGQlN5eERRVUZETEVOQlFVTXNRMEZCUXl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF5eEpRVUZKTEVWQlFVVXNRMEZCUXl4RFFVRkRMRU5CUVVNc1JVRkJSVHRUUVVOb1JTeERRVUZETzFGQlEwWXNUMEZCVHl4WFFVRlhMRU5CUVVNN1NVRkRja0lzUTBGQlF6dEpRVUZETEU5QlFVOHNRMEZCUXl4RlFVRkZMRU5CUVVNN1VVRkRXQ3hQUVVGUExFTkJRVU1zUjBGQlJ5eERRVU5VTERKRFFVRXlReXhGUVVNelF5eERRVUZETEVOQlFVTXNVVUZCVVN4RlFVRkZMRVZCUTFvc1VVRkJVU3hEUVVOVUxFTkJRVU03VVVGRFJpeFBRVUZQTEdGQlFXRXNRMEZCUXp0SlFVTjJRaXhEUVVGRE8wRkJRMGdzUTBGQlF6dEJRVVZFTEhWRVFVRjFSRHRCUVVOMlJDd3lSRUZCTWtRN1FVRkRNMFFzYVVWQlFXbEZPMEZCUTJwRkxHMUZRVUZ0UlR0QlFVTnVSU3gzUlVGQmQwVTdRVUZEZUVVc05rUkJRVFpFTzBGQlF6ZEVMSEZDUVVGeFFqdEJRVU55UWl3clJVRkJLMFU3UVVGREwwVXNVVUZCVVR0QlFVTlNMRWxCUVVrN1FVRkZTaXg1UTBGQmVVTTdRVUZEZWtNc1UwRkJVeXhSUVVGUkxFTkJRMllzZDBKQlFYZENMRVZCUVVVc1dVRkJXVHRCUVVOMFF5eExRVUZMTEVWQlFVVXNVMEZCVXp0QlFVTm9RaXhUUVVGVExFVkJRVVVzT0VOQlFUaERPMEZCUTNwRUxGZEJRVmNzUlVGQlJTeFRRVUZUTzBGQlEzUkNMR05CUVRKQ08wbEJRM3BDTEV0QlFVc3NSVUZCUlN4RFFVRkRPMGxCUTFJc2EwSkJRV3RDTEVWQlFVVXNSVUZCUlR0SlFVTjBRaXhaUVVGWkxFVkJRVVVzUzBGQlN6dEpRVU51UWl4bFFVRmxMRVZCUVVVc1MwRkJTenRKUVVOMFFpeHBRMEZCYVVNc1JVRkJSU3hGUVVGRk8wbEJRM0pETEZkQlFWY3NSVUZCUlN4TFFVRkxPMGxCUTJ4Q0xITkNRVUZ6UWl4RlFVRkZMRVZCUVVVN1NVRkRNVUlzVTBGQlV5eEZRVUZGTEV0QlFVczdTVUZEYUVJc2NVSkJRWEZDTEVWQlFVVXNTMEZCU3p0RFFVTTNRanRKUVVWRUxFbEJRVWtzUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVml4UFFVRlBPMGxCUTFRc1EwRkJRenRKUVVORUxFdEJRVXNzUjBGQlJ5eEpRVUZKTEVOQlFVTTdTVUZGWWl4TlFVRk5MRk5CUVZNc1IwRkJSeXd5UWtGQk1rSXNRMEZETTBNc1YwRkJWeXhEUVVGRExGTkJRVk1zUlVGRGNrSXNkMEpCUVhkQ0xFTkJRM3BDTEVOQlFVTTdTVUZEUml4SlFVRkpMRk5CUVZNc1JVRkJSU3hEUVVGRE8xRkJRMlFzUzBGQlN5eEhRVUZITEV0QlFVc3NRMEZCUXp0UlFVTmtMRTlCUVU4N1NVRkRWQ3hEUVVGRE8wbEJSVVFzVFVGQlRTeEhRVUZITEVkQlFVYzdVVUZEVml4VFFVRlRPMUZCUTFRc1RVRkJUU3hGUVVGRkxIZENRVUYzUWp0UlFVTm9ReXhMUVVGTExFVkJRVVVzWlVGQlpTeERRVUZETEV0QlFVc3NSVUZCUlN4WFFVRlhMRU5CUVVNc2NVSkJRWEZDTEVOQlFVTTdVVUZEYUVVc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eFRRVUZUTzFGQlEyaERMRlZCUVZVc1JVRkJSU3hYUVVGWExFTkJRVU1zVlVGQlZUdFJRVU5zUXl4VFFVRlRMRVZCUVVVc1YwRkJWeXhEUVVGRExGTkJRVk03VVVGRGFFTXNVVUZCVVN4RlFVRkZMRmRCUVZjc1EwRkJReXhSUVVGUk8xRkJRemxDTEdGQlFXRXNSVUZCUlN4WFFVRlhMRU5CUVVNc1lVRkJZVHRSUVVONFF5eFRRVUZUTEVWQlFVVXNWMEZCVnl4RFFVRkRMRk5CUVZNN1VVRkRhRU1zVDBGQlR5eEZRVUZGTEU5QlFVOHNSVUZCUlR0TFFVTnVRaXhEUVVGRE8wbEJSVVlzU1VGQlNTeERRVUZETzFGQlEwZ3NUVUZCVFN4RFFVRkRMRlZCUVZVc1JVRkJSU3hIUVVGSExFTkJRVU1zUTBGQlF6dEpRVU14UWl4RFFVRkRPMGxCUVVNc1QwRkJUeXhMUVVGTExFVkJRVVVzUTBGQlF6dFJRVU5tTEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc2EwTkJRV3RETEVOQlFVTXNRMEZCUXp0UlFVTm9SQ3hwUTBGQmFVTTdVVUZEYWtNc05FSkJRVFJDTzBsQlF6bENMRU5CUVVNN1NVRkZSQ3hMUVVGTExFZEJRVWNzUzBGQlN5eERRVUZETzBGQlEyaENMRU5CUVVNN1FVRkZSQ3huUWtGQlowSTdRVUZEYUVJc1UwRkJVeXhQUVVGUExFTkJRVU1zZDBKQlFYZENMRVZCUVVVc1NVRkJTU3hGUVVGRkxGZEJRVmM3U1VGRE1VUXNTVUZCU1N4TFFVRkxMRVZCUVVVc1EwRkJRenRSUVVOV0xFOUJRVTg3U1VGRFZDeERRVUZETzBsQlEwUXNTMEZCU3l4SFFVRkhMRWxCUVVrc1EwRkJRenRKUVVOaUxFMUJRVTBzVTBGQlV5eEhRVUZITERKQ1FVRXlRaXhEUVVNelF5eFhRVUZYTEVOQlFVTXNVMEZCVXl4RlFVTnlRaXgzUWtGQmQwSXNRMEZEZWtJc1EwRkJRenRKUVVOR0xFbEJRVWtzVTBGQlV5eEZRVUZGTEVOQlFVTTdVVUZEWkN4TFFVRkxMRWRCUVVjc1MwRkJTeXhEUVVGRE8xRkJRMlFzVDBGQlR6dEpRVU5VTEVOQlFVTTdTVUZEUkN4SlFVRkpMRU5CUVVNN1VVRkRTQ3h4UlVGQmNVVTdVVUZEY2tVc1RVRkJUU3hWUVVGVkxFZEJRVWNzUlVGQlJTeERRVUZETzFGQlEzUkNMRXRCUVVzc1RVRkJUU3hIUVVGSExFbEJRVWtzU1VGQlNTeEZRVUZGTEVOQlFVTTdXVUZEZGtJc1ZVRkJWU3hEUVVGRExFbEJRVWtzUTBGQlF5eGxRVUZsTEVOQlFVTXNSMEZCUnl4RlFVRkZMRXRCUVVzc1EwRkJReXhEUVVGRExFTkJRVU1zUTBGQlF5eDVSRUZCZVVRN1VVRkRla2NzUTBGQlF6dFJRVU5FTEUxQlFVMHNSMEZCUnl4SFFVRkhPMWxCUTFZc1UwRkJVeXhGUVVGRkxGZEJRVmNzUTBGQlF5eEpRVUZKTzFsQlF6TkNMRTFCUVUwc1JVRkJSU3gzUWtGQmQwSTdXVUZEYUVNc1NVRkJTU3hGUVVGRkxGVkJRVlU3V1VGRGFFSXNTMEZCU3l4RlFVRkZMRVZCUVVVN1dVRkRWQ3hUUVVGVExFVkJRVVVzVjBGQlZ5eERRVUZETEZOQlFWTTdXVUZEYUVNc1ZVRkJWU3hGUVVGRkxGZEJRVmNzUTBGQlF5eFZRVUZWTzFsQlEyeERMRk5CUVZNc1JVRkJSU3hYUVVGWExFTkJRVU1zVTBGQlV6dFpRVU5vUXl4UlFVRlJMRVZCUVVVc1YwRkJWeXhEUVVGRExGRkJRVkU3V1VGRE9VSXNZVUZCWVN4RlFVRkZMRmRCUVZjc1EwRkJReXhoUVVGaE8xbEJRM2hETEZOQlFWTXNSVUZCUlN4WFFVRlhMRU5CUVVNc1UwRkJVenRaUVVOb1F5eFBRVUZQTEVWQlFVVXNUMEZCVHl4RlFVRkZPMU5CUTI1Q0xFTkJRVU03VVVGRFJpeE5RVUZOTEVOQlFVTXNVMEZCVXl4RlFVRkZMRWRCUVVjc1EwRkJReXhEUVVGRE8wbEJRM3BDTEVOQlFVTTdTVUZCUXl4UFFVRlBMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMllzVDBGQlR5eERRVUZETEVkQlFVY3NRMEZCUXl4clEwRkJhME1zUjBGQlJ5eDNRa0ZCZDBJc1EwRkJReXhEUVVGRE8xRkJRek5GTEdsRFFVRnBRenRSUVVOcVF5eHpRa0ZCYzBJN1VVRkRkRUlzTkVKQlFUUkNPMGxCUXpsQ0xFTkJRVU03U1VGRFJDeExRVUZMTEVkQlFVY3NTMEZCU3l4RFFVRkRPMEZCUTJoQ0xFTkJRVU03UVVGclFrUXNUVUZCVFN4RFFVRkRMRk5CUVZNc1EwRkJReXh0UWtGQmJVSXNSMEZCUnl4VlFVRlZMRTlCUVU4c1JVRkJSU3hMUVVGTE8wbEJRemRFTEVsQlFVa3NUMEZCVHl4TFFVRkxMRk5CUVZNc1JVRkJSU3hEUVVGRE8xRkJRekZDTEUxQlFVMHNTVUZCU1N4TFFVRkxMRU5CUVVNc2QwTkJRWGRETEVOQlFVTXNRMEZCUXp0SlFVTTFSQ3hEUVVGRE8wbEJRMFFzU1VGQlNTeExRVUZMTEV0QlFVc3NVMEZCVXl4SlFVRkpMRTlCUVU4c1MwRkJTeXhMUVVGTExGRkJRVkVzUlVGQlJTeERRVUZETzFGQlEzSkVMRTFCUVUwc1NVRkJTU3hMUVVGTExFTkJRVU1zVVVGQlVTeEhRVUZITEV0QlFVc3NSMEZCUnl4aFFVRmhMRU5CUVVNc1EwRkJRenRKUVVOd1JDeERRVUZETzBsQlEwUXNTVUZCU1N4TFFVRkxMRWRCUVVjc1QwRkJUeXhEUVVGRE8wbEJRM0JDTEV0QlFVc3NTVUZCU1N4RFFVRkRMRWRCUVVjc1EwRkJReXhGUVVGRkxFTkJRVU1zU1VGQlNTeExRVUZMTEVWQlFVVXNRMEZCUXl4RlFVRkZMRVZCUVVVc1EwRkJRenRSUVVOb1F5eExRVUZMTEVkQlFVY3NUVUZCVFN4RFFVRkRMR05CUVdNc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dEpRVU4yUXl4RFFVRkRPMGxCUTBRc1NVRkJTU3hMUVVGTExFdEJRVXNzVTBGQlV5eEZRVUZGTEVOQlFVTTdVVUZEZUVJc1RVRkJUU3hKUVVGSkxFdEJRVXNzUTBGQlF5dzRRMEZCT0VNc1EwRkJReXhEUVVGRE8wbEJRMnhGTEVOQlFVTTdTVUZEUkN4UFFVRlBMRXRCUVVzc1EwRkJRenRCUVVObUxFTkJRVU1zUTBGQlF6dEJRVVZHT3pzN1IwRkhSenRCUVVOSUxFMUJRVTBzUTBGQlF5eFRRVUZUTEVOQlFVTXNkMEpCUVhkQ0xFZEJRVWNzVlVGQlZTeFBRVUZQTEVWQlFVVXNVVUZCVVN4SFFVRkhMRU5CUVVNN1NVRkRla1VzU1VGQlNTeFBRVUZQTEV0QlFVc3NVMEZCVXl4RlFVRkZMRU5CUVVNN1VVRkRNVUlzVFVGQlRTeEpRVUZKTEV0QlFVc3NRMEZCUXl4M1EwRkJkME1zUTBGQlF5eERRVUZETzBsQlF6VkVMRU5CUVVNN1NVRkRSQ3hOUVVGTkxFZEJRVWNzUjBGQlJ5eEZRVUZGTEVOQlFVTTdTVUZEWml4SlFVRkpMRXRCUVVzc1IwRkJSeXhEUVVGRExFTkJRVU03U1VGRFpDeEpRVUZKTEZWQlFWVXNSMEZCUnl4TlFVRk5MRU5CUVVNc2JVSkJRVzFDTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1NVRkRja1FzUjBGQlJ5eERRVUZETEVsQlFVa3NRMEZCUXl4RlFVRkZMRXRCUVVzc1JVRkJSU3hoUVVGaExFVkJRVVVzVlVGQlZTeEZRVUZGTEUxQlFVMHNSVUZCUlN4UFFVRlBMRVZCUVVVc1EwRkJReXhEUVVGRE8wbEJRMmhGTEVsQlFVa3NTMEZCU3l4SFFVRkhMRTFCUVUwc1EwRkJReXhqUVVGakxFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdTVUZGTTBNc1QwRkJUeXhMUVVGTExFdEJRVXNzU1VGQlNTeEpRVUZKTEV0QlFVc3NSMEZCUnl4UlFVRlJMRVZCUVVVc1EwRkJRenRSUVVNeFF5eExRVUZMTEVWQlFVVXNRMEZCUXp0UlFVTlNMRlZCUVZVc1IwRkJSeXhOUVVGTkxFTkJRVU1zYlVKQlFXMUNMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03VVVGREwwTXNSMEZCUnl4RFFVRkRMRWxCUVVrc1EwRkJReXhGUVVGRkxFdEJRVXNzUlVGQlJTeGhRVUZoTEVWQlFVVXNWVUZCVlN4RlFVRkZMRTFCUVUwc1JVRkJSU3hMUVVGTExFVkJRVVVzUTBGQlF5eERRVUZETzFGQlF6bEVMRXRCUVVzc1IwRkJSeXhOUVVGTkxFTkJRVU1zWTBGQll5eERRVUZETEV0QlFVc3NRMEZCUXl4RFFVRkRPMGxCUTNaRExFTkJRVU03U1VGRFJDeFBRVUZQTEVkQlFVY3NRMEZCUXp0QlFVTmlMRU5CUVVNc1EwRkJRenRCUVVWR096dEhRVVZITzBGQlEwZ3NUVUZCVFN4RFFVRkRMRk5CUVZNc1EwRkJReXh0UWtGQmJVSXNSMEZCUnl4VlFVRlZMRTlCUVU4c1JVRkJSU3haUVVGWk8wbEJRM0JGTEVsQlFVa3NUMEZCVHl4TFFVRkxMRk5CUVZNc1NVRkJTU3haUVVGWkxFdEJRVXNzVTBGQlV5eEZRVUZGTEVOQlFVTTdVVUZEZUVRc1RVRkJUU3hKUVVGSkxFdEJRVXNzUTBGQlF5d3dRMEZCTUVNc1EwRkJReXhEUVVGRE8wbEJRemxFTEVOQlFVTTdTVUZEUkN4SlFVRkpMRlZCUVZVc1IwRkJSeXhGUVVGRkxFTkJRVU03U1VGRGNFSXNTVUZCU1N4TFFVRkxMRWRCUVVjc1EwRkJReXhEUVVGRE8wbEJRMlFzVDBGQlR5eFBRVUZQTEV0QlFVc3NTVUZCU1N4RlFVRkZMRU5CUVVNN1VVRkRlRUlzVlVGQlZTeEhRVUZITEUxQlFVMHNRMEZCUXl4dFFrRkJiVUlzUTBGQlF5eFBRVUZQTEVOQlFVTXNRMEZCUXp0UlFVTnFSQ3hKUVVGSkxGVkJRVlVzUTBGQlF5eFJRVUZSTEVOQlFVTXNXVUZCV1N4RFFVRkRMRVZCUVVVc1EwRkJRenRaUVVOMFF5eFBRVUZQTEVWQlFVVXNTMEZCU3l4RlFVRkZMRmxCUVZrc1JVRkJSU3hEUVVGRE8xRkJRMnBETEVOQlFVTTdVVUZEUkN4TFFVRkxMRVZCUVVVc1EwRkJRenRSUVVOU0xFOUJRVThzUjBGQlJ5eE5RVUZOTEVOQlFVTXNZMEZCWXl4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRE8wbEJRek5ETEVOQlFVTTdTVUZEUkN4TlFVRk5MRXRCUVVzc1EwRkJReXcyUkVGQk5rUXNRMEZCUXl4RFFVRkRPMEZCUXpkRkxFTkJRVU1zUTBGQlF6dEJRVVZHT3p0SFFVVkhPMEZCUTBnc1UwRkJVeXd3UWtGQk1FSXNRMEZCUXl4SlFVRkpPMGxCUTNSRExFMUJRVTBzUjBGQlJ5eEhRVUZITEVWQlFVVXNRMEZCUXp0SlFVTm1MRWxCUVVrc1EwRkJReXhYUVVGWExFTkJRVU1zY1VKQlFYRkNMRU5CUVVNc1QwRkJUeXhEUVVGRExFTkJRVU1zUjBGQlJ5eEZRVUZGTEVWQlFVVTdVVUZEY2tRc1IwRkJSeXhEUVVGRExFbEJRVWtzUTBGQlF5eEhRVUZITEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNN1NVRkRjRUlzUTBGQlF5eERRVUZETEVOQlFVTTdTVUZEU0N4UFFVRlBMRWRCUVVjc1EwRkJRenRCUVVOaUxFTkJRVU03UVVGRlJDeFRRVUZUTEdkRFFVRm5ReXhEUVVGRExFOUJRVThzUlVGQlJTeGpRVUZqTzBsQlF5OUVMRTFCUVUwc1IwRkJSeXhIUVVGSExFOUJRVThzUTBGQlF5eGpRVUZqTEVOQlFVTXNRMEZCUXp0SlFVTndReXhKUVVGSkxFZEJRVWNzUlVGQlJTeERRVUZETzFGQlExSXNUMEZCVHl4SFFVRkhMRU5CUVVNc1UwRkJVeXhEUVVGRExFTkJRVU1zUTBGQlF5eEhRVUZITEVOQlFVTXNVMEZCVXl4RFFVRkRMRU5CUVVNc1EwRkJReXhOUVVGTkxFTkJRVU1zWTBGQll5eERRVUZETEVkQlFVY3NRMEZCUXl4RFFVRkRPMGxCUTNCRkxFTkJRVU03VTBGQlRTeERRVUZETzFGQlEwNHNUMEZCVHl4VFFVRlRMRU5CUVVNN1NVRkRia0lzUTBGQlF6dEJRVU5JTEVOQlFVTTdRVUZGUkRzN096dEhRVWxITzBGQlEwZ3NVMEZCVXl4dFFrRkJiVUlzUTBGQlF5eFBRVUZQTEVWQlFVVXNTVUZCU1R0SlFVTjRReXhKUVVGSkxITkNRVUZ6UWl4SFFVRkhMRWxCUVVrc1EwRkJReXhYUVVGWExFTkJRVU1zYzBKQlFYTkNMRU5CUVVNN1NVRkRja1VzVFVGQlRTeExRVUZMTEVkQlFVY3NaME5CUVdkRExFTkJRVU1zVDBGQlR5eEZRVUZGTEVsQlFVa3NRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkJRenRKUVVOeVJTeEpRVUZKTEVOQlFVTXNTMEZCU3l4RlFVRkZMRU5CUVVNN1VVRkRXQ3hOUVVGTkxFdEJRVXNzUTBGQlF5eFRRVUZUTEVkQlFVY3NTVUZCU1N4RFFVRkRMRTFCUVUwc1IwRkJSeXhwUWtGQmFVSXNRMEZCUXl4RFFVRkRPMGxCUXpORUxFTkJRVU03U1VGRlJDeEpRVUZKTEhOQ1FVRnpRaXhMUVVGTExGTkJRVk1zU1VGQlNTeERRVUZETEhOQ1FVRnpRaXhEUVVGRExFMUJRVTBzUlVGQlJTeERRVUZETzFGQlF6TkZMSE5DUVVGelFpeEhRVUZITEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGQlF5eExRVUZMTEVWQlFVVXNTVUZCU1N4RFFVRkRMRXRCUVVzc1EwRkJReXhEUVVGRE8xRkJRelZGTERaRFFVRTJRenRSUVVNM1F5eE5RVUZOTEZGQlFWRXNSMEZCUnl3d1FrRkJNRUlzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXl4TlFVRk5MRU5CUTNSRUxFbEJRVWtzUTBGQlF5eFhRVUZYTEVOQlFVTXNhMEpCUVd0Q0xFTkJRM0JETEVOQlFVTTdVVUZEUml4elFrRkJjMElzUjBGQlJ5eDNRa0ZCZDBJc1EwRkRMME1zYzBKQlFYTkNMRVZCUTNSQ0xGRkJRVkVzUTBGRFZDeERRVUZETzBsQlEwb3NRMEZCUXp0VFFVRk5MRU5CUVVNN1VVRkRUaXhyUTBGQmEwTTdVVUZEYkVNc2MwSkJRWE5DTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNc1dVRkJXU3hGUVVGRkxFVkJRVVU3V1VGRE9VTXNXVUZCV1N4RFFVRkRMRTFCUVUwc1IwRkJSeXhOUVVGTkxFTkJRVU1zYlVKQlFXMUNMRU5CUXpsRExFdEJRVXNzUlVGRFRDeFpRVUZaTEVOQlFVTXNTMEZCU3l4RFFVTnVRaXhEUVVGRE8xRkJRMG9zUTBGQlF5eERRVUZETEVOQlFVTTdTVUZEVEN4RFFVRkRPMGxCUTBRc1QwRkJUeXh6UWtGQmMwSXNRMEZCUXp0QlFVTm9ReXhEUVVGRE8wRkJSVVE3T3pzN096dEhRVTFITzBGQlEwZ3NVMEZCVXl4TlFVRk5MRU5CUVVNc1NVRkJTU3hGUVVGRkxFOUJRVTg3U1VGRE0wSXNUMEZCVHl4RFFVRkRMRk5CUVZNc1IwRkJSeXhKUVVGSkxFbEJRVWtzUlVGQlJTeERRVUZETEZkQlFWY3NSVUZCUlN4RFFVRkRPMGxCUXpkRExFOUJRVThzUTBGQlF5eFBRVUZQTEVOQlFVTXNWMEZCVnl4RFFVRkRPMUZCUXpGQ0xGTkJRVk1zUlVGQlJTdzBRa0ZCTkVJN1VVRkRka01zU1VGQlNUdFJRVU5LTEVsQlFVa3NSVUZCUlN4UFFVRlBPMHRCUTJRc1EwRkJReXhEUVVGRE8wRkJRMHdzUTBGQlF6dEJRVVZFTEZOQlFWTXNkMEpCUVhkQ0xFTkJReTlDTEZWQlFXOURMRVZCUTNCRExGRkJRV0U3U1VGRllpeExRVUZMTEUxQlFVMHNTVUZCU1N4SlFVRkpMRlZCUVZVc1JVRkJSU3hEUVVGRE8xRkJRemxDTEVsQlFVa3NRMEZCUXl4aFFVRmhMRWRCUVVjc1NVRkJTU3hEUVVGRExHRkJRV0VzUTBGQlF5eE5RVUZOTEVOQlF6VkRMRU5CUVVNc1EwRkJReXhGUVVGRkxFVkJRVVVzUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4UlFVRlJMRU5CUVVNc1EwRkJReXhEUVVGRExFTkJRemRDTEVOQlFVTTdTVUZEU2l4RFFVRkRPMGxCUTBRc1QwRkJUeXhWUVVGVkxFTkJRVU03UVVGRGNFSXNRMEZCUXp0QlFVVkVPenM3T3pzN1IwRk5SenRCUVVOSUxGTkJRVk1zYjBKQlFXOUNMRU5CUVVNc1NVRkJTU3hGUVVGRkxFOUJRVThzUlVGQlJTeEpRVUZKTzBsQlF5OURMRTFCUVUwc1dVRkJXU3hIUVVGSExFOUJRVThzUTBGQlF5eGxRVUZsTEVOQlFVTXNUVUZCVFN4RFFVRkRMRTFCUVUwc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF6dEpRVU5xUlN4TlFVRk5MRmRCUVZjc1IwRkJSeXhqUVVGakxFTkJRVU1zU1VGQlNTeEZRVUZGTEZsQlFWa3NSVUZCUlR0UlFVTnlSQ3g1UWtGQmVVSXNSVUZCUlN4SlFVRkpPMUZCUXk5Q0xGRkJRVkVzUlVGQlJTeEpRVUZKTzB0QlEyWXNRMEZCUXl4RFFVRkRPMGxCUTBnc1QwRkJUeXhYUVVGWExFTkJRVU03UVVGRGNrSXNRMEZCUXp0QlFVVkVPenRIUVVWSE8wRkJSVWdzVTBGQlV5eGpRVUZqTEVOQlEzSkNMRzlDUVVGdlFpeEZRVU53UWl4VlFVRlZMRVZCUTFZc1dVRkJXU3hGUVVOYUxGVkJRVlVzUlVGRFZpeFpRVUZaTzBsQlJWb3NUVUZCVFN4blFrRkJaMElzUjBGQlJ5eHZRa0ZCYjBJc1EwRkRNME1zYjBKQlFXOUNMRVZCUTNCQ0xFMUJRVTBzUlVGRFRpeFpRVUZaTEVOQlEySXNRMEZCUXp0SlFVTkdMR05CUVdNc1EwRkRXaXhWUVVGVkxFVkJRMVlzVlVGQlZTeEZRVU5XTEZsQlFWa3NSVUZEV2l4WlFVRlpMRVZCUTFvc1owSkJRV2RDTEVOQlEycENMRU5CUVVNN1FVRkRTaXhEUVVGRE8wRkJSVVE3T3pzN096czdSMEZQUnp0QlFVTklMRk5CUVZNc01rSkJRVEpDTEVOQlEyeERMRlZCUVZVc1JVRkRWaXhSUVVGUkxFVkJRMUlzVVVGQlVTeEZRVU5TTEUxQlFVMHNSVUZEVGl4SlFVRkpPMGxCUlVvc1RVRkJUU3hoUVVGaExFZEJRVWNzVVVGQlVTeERRVUZETEVsQlFVa3NRMEZCUXl4TlFVRk5MRVZCUVVVc1IwRkJSeXhKUVVGSkxFTkJRVU1zUTBGQlF6dEpRVU55UkN4TlFVRk5MRmRCUVZjc1IwRkJSeXd5UWtGQk1rSXNRMEZCUXl4SlFVRkpMRU5CUVVNc1EwRkJRenRKUVVOMFJDeE5RVUZOTEZkQlFWY3NSMEZCUnl4UlFVRlJMRXRCUVVzc1UwRkJVeXhEUVVGRExFTkJRVU1zUTBGQlF5eFJRVUZSTEVOQlFVTXNRMEZCUXl4RFFVRkRMR0ZCUVdFc1EwRkJRenRKUVVOMFJTeFJRVUZSTEVOQlEwNHNWVUZCVlN4RlFVTldMRmRCUVZjc1JVRkRXQ3hYUVVGWExFTkJRVU1zUjBGQlJ5eEZRVU5tTEZkQlFWY3NRMEZGV2l4RFFVRkRPMGxCUTBZc1QwRkJUeXhYUVVGWExFTkJRVU03UVVGRGNrSXNRMEZCUXp0QlFVTkVPenM3T3pzN1IwRk5SenRCUVVOSUxGTkJRVk1zTWtKQlFUSkNMRU5CUTJ4RExGVkJRVlVzUlVGRFZpeFJRVUZSTEVWQlExSXNVVUZCVVN4RlFVTlNMRTFCUVUwc1JVRkRUaXhMUVVGTE8wbEJSVXdzVFVGQlRTeFhRVUZYTEVkQlFVY3NNa0pCUVRKQ0xFTkJRVU1zU1VGQlNTeERRVUZETEVOQlFVTTdTVUZEZEVRc1VVRkJVU3hEUVVOT0xGVkJRVlVzUlVGRFZpeFJRVUZSTEVWQlExSXNVVUZCVVN4RFFVRkRMRU5CUVVNc1EwRkJReXhYUVVGWExFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTXNRMEZCUXl4WFFVRlhMRU5CUVVNc1ZVRkJWU3hGUVVOdVJDeFhRVUZYTEVOQlJWb3NRMEZCUXp0SlFVTkdMRTlCUVU4c1EwRkJReXhSUVVGUkxFTkJRVU1zUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRMRU5CUVVNc1VVRkJVU3hEUVVGRExFbEJRVWtzUTBGQlF5eE5RVUZOTEVWQlFVVXNVVUZCVVN4RFFVRkRMRU5CUVVNN1FVRkRhRVVzUTBGQlF6dEJRVVZFT3pzN096czdSMEZOUnp0QlFVTklMRk5CUVZNc1kwRkJZeXhEUVVOeVFpeFZRVUZWTEVWQlExWXNWVUZCVlN4RlFVTldMRmxCUVZrc1JVRkRXaXhSUVVGUkxFZEJRVWNzVTBGQlV6dEpRVVZ3UWl4TlFVRk5MRkZCUVZFc1IwRkJSeXhWUVVGVkxFTkJRVU1zUjBGQlJ5eERRVUZETzBsQlEyaERMRTlCUVU4c1RVRkJUU3hEUVVGRExIZENRVUYzUWl4RFFVTndRenRSUVVORkxFbEJRVWtzUTBGQlF5eFpRVUZaTEVOQlFVTTdXVUZEYUVJc1QwRkJUeXd5UWtGQk1rSXNRMEZEYUVNc1ZVRkJWU3hGUVVOV0xGRkJRVkVzUlVGRFVpeFJRVUZSTEVWQlExSXNTVUZCU1N4RlFVTktMRk5CUVZNc1EwRkRWaXhEUVVGRE8xRkJRMG9zUTBGQlF6dExRVU5HTEVWQlEwUXNXVUZCV1N4RFFVTmlMRU5CUVVNc1IwRkJSeXhEUVVGRE8wRkJRMUlzUTBGQlF6dEJRVVZFT3pzN096czdSMEZOUnp0QlFVTklMRk5CUVZNc1kwRkJZeXhEUVVOeVFpeFZRVUZWTEVWQlExWXNWVUZCVlN4RlFVTldMRmxCUVZrc1JVRkRXaXhaUVVFMlFpeFRRVUZUTzBsQlJYUkRMRTFCUVUwc1VVRkJVU3hIUVVGSExGVkJRVlVzUTBGQlF5eEhRVUZITEVOQlFVTTdTVUZEYUVNc1QwRkJUeXhOUVVGTkxFTkJRVU1zZDBKQlFYZENMRU5CUTNCRE8xRkJRMFVzUjBGQlJ5eERRVUZETEVkQlFVY3NSVUZCUlN4TFFVRkxMRVZCUVVVc1MwRkJTenRaUVVOdVFpeDVRa0ZCZVVJN1dVRkRla0lzVDBGQlR5d3lRa0ZCTWtJc1EwRkRhRU1zVlVGQlZTeEZRVU5XTEZGQlFWRXNSVUZEVWl4TFFVRkxMRVZCUTB3c1IwRkJSeXhGUVVOSUxGTkJRVk1zUTBGRFZpeERRVUZETzFGQlEwb3NRMEZCUXp0TFFVTkdMRVZCUTBRc1dVRkJXU3hEUVVOaUxFTkJRVU1zUjBGQlJ5eERRVUZETzBGQlExSXNRMEZCUXp0QlFVVkVPenM3UjBGSFJ6dEJRVU5JTEZOQlFWTXNZMEZCWXl4RFFVRkRMRlZCUVZVc1JVRkJSU3hWUVVGVkxFVkJRVVVzU1VGQlNTeEZRVUZGTEUxQlFVMHNSVUZCUlN4UFFVRlBPMGxCUTI1RkxGVkJRVlVzUTBGQlF5eE5RVUZOTEVOQlFVTXNSMEZCUnl4UFFVRlBMRU5CUVVNN1NVRkROMElzVFVGQlRTeERRVUZETEdOQlFXTXNRMEZCUXl4VlFVRlZMRVZCUVVVc1NVRkJTU3hGUVVGRkxGVkJRVlVzUTBGQlF5eERRVUZETzBGQlEzUkVMRU5CUVVNN1FVRkZSRHM3T3pzN1IwRkxSenRCUVVOSUxGTkJRVk1zYzBKQlFYTkNMRU5CUVVNc1QwRkJUeXhGUVVGRkxHTkJRV003U1VGRGNrUXNTVUZCU1N4UFFVRlBMRXRCUVVzc1UwRkJVeXhKUVVGSkxHTkJRV01zUzBGQlN5eFRRVUZUTEVWQlFVVXNRMEZCUXp0UlFVTXhSQ3hQUVVGUE8wbEJRMVFzUTBGQlF6dEpRVU5FTEU5QlFVOHNUMEZCVHl4RFFVRkRMR05CUVdNc1EwRkJReXhEUVVGRExGTkJRVk1zU1VGQlNTeFBRVUZQTEVOQlFVTXNZMEZCWXl4RFFVRkRMRU5CUVVNN1FVRkRkRVVzUTBGQlF6dEJRVVZFT3pzN08wZEJTVWM3UVVGRFNDeFRRVUZUTEhOQ1FVRnpRaXhEUVVNM1FpeFZRVUZWTEVWQlExWXNWVUZCVlN4RlFVTldMRlZCUVZVc1JVRkRWaXhaUVVGWkxFVkJRMW9zVVVGQlVTeEhRVUZITEZOQlFWTTdTVUZGY0VJc1NVRkJTU3h2UWtGQmIwSXNRMEZCUXp0SlFVTjZRaXhOUVVGTkxGZEJRVmNzUjBGQlJ5eExRVUZMTEVOQlFVTTdTVUZETVVJc1RVRkJUU3hYUVVGWExFZEJRVWNzUzBGQlN5eERRVUZETzBsQlJURkNMRWxCUVVrc1RVRkJUU3hEUVVGRExGTkJRVk1zUTBGQlF5eGpRVUZqTEVOQlFVTXNTVUZCU1N4RFFVRkRMRlZCUVZVc1JVRkJReXhYUVVGWExFTkJRVU1zUlVGQlJTeERRVUZETzFGQlEycEZMRzlDUVVGdlFpeEhRVUZITEdOQlFXTXNRMEZEYmtNc1ZVRkJWU3hGUVVOV0xGVkJRVlVzUlVGRFZpeFpRVUZaTEVWQlExb3NVVUZCVVN4RFFVTlVMRU5CUVVNN1VVRkRSaXhqUVVGakxFTkJRMW9zYjBKQlFXOUNMRVZCUTNCQ0xGVkJRVlVzUlVGRFZpeFhRVUZYTEVWQlExZ3NWVUZCVlN4RlFVTldMRmxCUVZrc1EwRkRZaXhEUVVGRE8wbEJRMG9zUTBGQlF6dEpRVU5FTEVsQlFVa3NUVUZCVFN4RFFVRkRMRk5CUVZNc1EwRkJReXhqUVVGakxFTkJRVU1zU1VGQlNTeERRVUZETEZWQlFWVXNSVUZCUXl4WFFVRlhMRU5CUVVNc1JVRkJSU3hEUVVGRE8xRkJRMnBGTEc5Q1FVRnZRaXhIUVVGSExHTkJRV01zUTBGQlF5eFZRVUZWTEVWQlFVVXNWVUZCVlN4RlFVRkZMRmxCUVZrc1EwRkJReXhEUVVGRE8xRkJRelZGTEdOQlFXTXNRMEZEV2l4dlFrRkJiMElzUlVGRGNFSXNWVUZCVlN4RlFVTldMRmRCUVZjc1JVRkRXQ3hWUVVGVkxFVkJRMVlzV1VGQldTeERRVU5pTEVOQlFVTTdTVUZEU2l4RFFVRkRPMEZCUTBnc1EwRkJRenRCUVVWRU96dEhRVVZITzBGQlEwZ3NVMEZCVXl4cFFrRkJhVUlzUTBGQlF5eFJRVUZSTEVWQlFVVXNWVUZCVlN4RlFVRkZMRkZCUVZFc1JVRkJSU3hUUVVGVE8wbEJRMnhGTEZOQlFWTXNTVUZCU1R0UlFVTllMRWxCUVVrc1RVRkJUU3hEUVVGRE8xRkJRMWdzVFVGQlRTeFhRVUZYTEVkQlFVY3NNa0pCUVRKQ0xFTkJRVU1zU1VGQlNTeEZRVUZGTEVsQlFVa3NRMEZCUXl4RFFVRkRPMUZCUXpWRUxFOUJRVThzUTBGQlF5eFZRVUZWTEVWQlFVVXNVMEZCVXl4RlFVRkZMRmRCUVZjc1EwRkJReXhEUVVGRE8xRkJRelZETEVsQlFVa3NRMEZCUXp0WlFVTklMRTFCUVUwN1owSkJRMG9zVTBGQlV5eERRVUZETEUxQlFVMHNSMEZCUnl4RFFVRkRPMjlDUVVOc1FpeERRVUZETEVOQlFVTXNVVUZCVVN4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFVkJRVVVzUjBGQlJ5eFRRVUZUTEVOQlFVTTdiMEpCUTI1RExFTkJRVU1zUTBGQlF5eFJRVUZSTEVOQlFVTXNTVUZCU1N4RFFVRkRMRWxCUVVrc1EwRkJReXhEUVVGRE8xRkJRelZDTEVOQlFVTTdVVUZCUXl4UFFVRlBMRWRCUVVjc1JVRkJSU3hEUVVGRE8xbEJRMklzVFVGQlRTeFRRVUZUTEVkQlFVY3NTVUZCUVN3eVFrRkJiVUlzUlVGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0WlFVTXpReXhOUVVGTkxGTkJRVk1zUTBGQlF6dFJRVU5zUWl4RFFVRkRPMUZCUTBRc1QwRkJUeXhOUVVGTkxFTkJRVU03U1VGRGFFSXNRMEZCUXp0SlFVTkVMRTlCUVU4c1NVRkJTU3hEUVVGRE8wRkJRMlFzUTBGQlF6dEJRVVZFT3p0SFFVVkhPMEZCUTBnc1UwRkJVeXhyUWtGQmEwSXNRMEZEZWtJc1QwRkJUeXhGUVVOUUxGVkJRVlVzUlVGRFZpeFZRVUZWTEVWQlExWXNWVUZCVlN4RlFVTldMRmxCUVZrN1NVRkZXaXhOUVVGTkxGRkJRVkVzUjBGQlJ5eFZRVUZWTEVOQlFVTXNTMEZCU3l4RFFVRkRPMGxCUTJ4RExFMUJRVTBzV1VGQldTeEhRVUZITEdsQ1FVRnBRaXhEUVVOd1F5eFBRVUZQTEVWQlExQXNWVUZCVlN4RlFVTldMRkZCUVZFc1JVRkRVaXhaUVVGWkxFTkJRMklzUTBGQlF6dEpRVU5HTEUxQlFVMHNaMEpCUVdkQ0xFZEJRVWNzYjBKQlFXOUNMRU5CUXpORExGbEJRVmtzUlVGRFdpeFBRVUZQTEVWQlExQXNVVUZCVVN4RFFVRkRMRWxCUVVrc1EwRkRaQ3hEUVVGRE8wbEJRMFlzWTBGQll5eERRVU5hTEZWQlFWVXNSVUZEVml4VlFVRlZMRVZCUTFZc1dVRkJXU3hGUVVOYUxFOUJRVThzUlVGRFVDeG5Ra0ZCWjBJc1EwRkRha0lzUTBGQlF6dEJRVU5LTEVOQlFVTTdRVUZGUkRzN096czdSMEZMUnp0QlFVTklMRk5CUVZNc1ZVRkJWU3hEUVVGRExFOUJRVThzUlVGQlJTeEpRVUZKTEVWQlFVVXNTMEZCU3l4RlFVRkZMRmxCUVZrc1JVRkJSU3hSUVVGUkxFZEJRVWNzVTBGQlV6dEpRVU14UlN4SlFVRkpMRU5CUVVNN1VVRkRTQ3hOUVVGTkxGVkJRVlVzUjBGQlJ5eEpRVUZKTEVOQlFVTXNaMEpCUVdkQ0xFZEJRVWNzUjBGQlJ5eEhRVUZITEZsQlFWa3NRMEZCUXp0UlFVTTVSQ3hOUVVGTkxHbENRVUZwUWl4SFFVRkhMSE5DUVVGelFpeERRVU01UXl4UFFVRlBMRU5CUVVNc1pVRkJaU3hGUVVOMlFpeEpRVUZKTEVOQlFVTXNUVUZCVFN4RFFVTmFMRU5CUVVNN1VVRkRSaXhOUVVGTkxGVkJRVlVzUjBGQlJ5eE5RVUZOTEVOQlFVTXNiVUpCUVcxQ0xFTkJRVU1zYVVKQlFXbENMRVZCUVVVc1MwRkJTeXhEUVVGRExFTkJRVU03VVVGRGVFVXNUVUZCVFN4VlFVRlZMRWRCUVVjc1RVRkJUU3hEUVVGRExIRkNRVUZ4UWl4RFFVRkRMRlZCUVZVc1JVRkJSU3haUVVGWkxFTkJRVU1zUTBGQlF6dFJRVU14UlN4SlFVRkpMRlZCUVZVc1MwRkJTeXhUUVVGVExFVkJRVVVzUTBGQlF6dFpRVU0zUWl3d1JFRkJNRVE3V1VGRE1VUXNUMEZCVHp0UlFVTlVMRU5CUVVNN1VVRkRSQ3hKUVVGSkxFOUJRVThzVlVGQlZTeERRVUZETEV0QlFVc3NTMEZCU3l4VlFVRlZMRVZCUVVVc1EwRkJRenRaUVVNelF5eHJRa0ZCYTBJc1EwRkRhRUlzVDBGQlR5eEZRVU5RTEZWQlFWVXNSVUZEVml4VlFVRlZMRVZCUTFZc1ZVRkJWU3hGUVVOV0xGbEJRVmtzUTBGRFlpeERRVUZETzFGQlEwb3NRMEZCUXp0aFFVRk5MRU5CUVVNN1dVRkRUaXh6UWtGQmMwSXNRMEZEY0VJc1ZVRkJWU3hGUVVOV0xGVkJRVlVzUlVGRFZpeFZRVUZWTEVWQlExWXNXVUZCV1N4RlFVTmFMRkZCUVZFc1EwRkRWQ3hEUVVGRE8xRkJRMG9zUTBGQlF6dEpRVU5JTEVOQlFVTTdTVUZCUXl4UFFVRlBMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMllzVDBGQlR5eERRVUZETEV0QlFVc3NRMEZCUXl4TFFVRkxMRU5CUVVNc1EwRkJRenRSUVVOeVFpeFBRVUZQTEVOQlFVTXNTMEZCU3l4RFFVRkRMRXRCUVVzc1EwRkJReXhMUVVGTExFTkJRVU1zUTBGQlF6dFJRVU16UWl4UFFVRlBPMGxCUTFRc1EwRkJRenRCUVVOSUxFTkJRVU03UVVGRlJEczdPMGRCUjBjN1FVRkRTQ3hOUVVGTkxHTkJRV01zUjBGQlJ5eEZRVUZGTEVOQlFVTTdRVUZETVVJc1UwRkJVeXhaUVVGWkxFTkJRVU1zVFVGQlRUdEpRVU14UWl4SlFVRkpMR05CUVdNc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF5eEhRVUZITEVWQlFVVXNSVUZCUlN4RFFVRkRMRTFCUVUwc1MwRkJTeXhIUVVGSExFTkJRVU1zUlVGQlJTeERRVUZETzFGQlEycEVMRTlCUVU4c1MwRkJTeXhEUVVGRE8wbEJRMllzUTBGQlF6dEpRVU5FTEdOQlFXTXNRMEZCUXl4SlFVRkpMRU5CUVVNc1RVRkJUU3hEUVVGRExFTkJRVU03U1VGRE5VSXNUMEZCVHl4SlFVRkpMRU5CUVVNN1FVRkRaQ3hEUVVGRE8wRkJSVVFzVTBGQlV5eGxRVUZsTEVOQlFVTXNUMEZCVHp0SlFVTTVRaXhMUVVGTExFMUJRVTBzU1VGQlNTeEpRVUZKTEc5RFFVRjVRaXhGUVVGRkxFTkJRVU03VVVGRE4wTXNOa05CUVRaRE8xRkJRemRETEVsQlFVa3NhMEpCUVd0Q0xFTkJRVU03VVVGRGRrSXNTVUZCU1N4RFFVRkRPMWxCUTBnc2EwSkJRV3RDTEVkQlFVY3NiVUpCUVcxQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEVsQlFVa3NRMEZCUXl4RFFVRkRPMUZCUXpGRUxFTkJRVU03VVVGQlF5eFBRVUZQTEVkQlFVY3NSVUZCUlN4RFFVRkRPMWxCUTJJc1QwRkJUeXhEUVVGRExFdEJRVXNzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0WlFVTnVRaXhUUVVGVE8xRkJRMWdzUTBGQlF6dFJRVU5FTEdkRVFVRm5SRHRSUVVOb1JDeEpRVUZKTEd0Q1FVRnJRaXhEUVVGRExFTkJRVU1zUTBGQlF5eExRVUZMTEVWQlFVVXNSVUZCUlN4RFFVRkRPMWxCUTJwRExHdENRVUZyUWl4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRExFVkJRVVVzUzBGQlN5eEZRVUZGTEdGQlFXRXNSVUZCUlN4TlFVRk5MRVZCUVVVc1JVRkJSU3hGUVVGRk8yZENRVU01UkN4SlFVRkpMRmxCUVZrc1EwRkJReXhOUVVGTkxFTkJRVU1zUlVGQlJTeERRVUZETzI5Q1FVTjZRaXhoUVVGaExFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTXNXVUZCV1N4RlFVRkZMRVZCUVVVc1EwRkRja01zVlVGQlZTeERRVUZETEU5QlFVOHNSVUZCUlN4SlFVRkpMRVZCUVVVc1MwRkJTeXhGUVVGRkxGbEJRVmtzUTBGQlF5eERRVU12UXl4RFFVRkRPMmRDUVVOS0xFTkJRVU03V1VGRFNDeERRVUZETEVOQlFVTXNRMEZCUXp0UlFVTk1MRU5CUVVNN1VVRkRSQ3g1UkVGQmVVUTdVVUZEZWtRc1NVRkJTU3hKUVVGSkxFTkJRVU1zVjBGQlZ5eERRVUZETEhGQ1FVRnhRaXhGUVVGRkxFTkJRVU03V1VGRE0wTXNTVUZCU1N4RFFVRkRMRmRCUVZjc1EwRkJReXh4UWtGQmNVSXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJReXhGUVVGRkxFZEJRVWNzUlVGQlJTeEpRVUZKTEVWQlFVVXNTMEZCU3l4RlFVRkZMRVZCUVVVc1JVRkJSVHRuUWtGRGRFVXNUVUZCVFN4TFFVRkxMRWRCUVVjc1owTkJRV2RETEVOQlFVTXNUMEZCVHl4RlFVRkZMRWxCUVVrc1EwRkJReXhOUVVGTkxFTkJRVU1zUTBGQlF6dG5Ra0ZEY2tVc1NVRkJTU3hMUVVGTExFVkJRVVVzUTBGQlF6dHZRa0ZEVml4TlFVRk5MRVZCUVVVc1MwRkJTeXhGUVVGRkxGbEJRVmtzUlVGQlJTeEhRVUZITEUxQlFVMHNRMEZCUXl4dFFrRkJiVUlzUTBGRGVFUXNTMEZCU3l4RlFVTk1MRWxCUVVrc1EwRkRUQ3hEUVVGRE8yOUNRVU5HTEZWQlFWVXNRMEZCUXl4UFFVRlBMRVZCUVVVc1NVRkJTU3hGUVVGRkxFdEJRVXNzUlVGQlJTeFpRVUZaTEVWQlFVVXNTMEZCU3l4RFFVRkRMRU5CUVVNN1owSkJRM2hFTEVOQlFVTTdjVUpCUVUwc1EwRkJRenR2UWtGRFRpeFBRVUZQTEVOQlFVTXNTMEZCU3l4RFFVTllMSFZDUVVGMVFqdDNRa0ZEY2tJc1NVRkJTU3hEUVVGRExFMUJRVTA3ZDBKQlExZ3NhVU5CUVdsRExFTkJRM0JETEVOQlFVTTdaMEpCUTBvc1EwRkJRenRaUVVOSUxFTkJRVU1zUTBGQlF5eERRVUZETzFGQlEwd3NRMEZCUXp0SlFVTklMRU5CUVVNN1FVRkRTQ3hEUVVGREluMD0iLCJcInVzZSBzdHJpY3RcIjtcbk9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCBcIl9fZXNNb2R1bGVcIiwgeyB2YWx1ZTogdHJ1ZSB9KTtcbmV4cG9ydHMuanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyA9IHZvaWQgMDtcbmV4cG9ydHMuanNJbnN0cnVtZW50YXRpb25TZXR0aW5ncyA9IFtcbiAgICB7XG4gICAgICAgIG9iamVjdDogXCJTY3JpcHRQcm9jZXNzb3JOb2RlXCIsIC8vIERlcGNyZWNhdGVkLiBSZXBsYWNlZCBieSBBdWRpb1dvcmtsZXROb2RlXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiU2NyaXB0UHJvY2Vzc29yTm9kZVwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiQXVkaW9Xb3JrbGV0Tm9kZVwiLFxuICAgICAgICBpbnN0cnVtZW50ZWROYW1lOiBcIkF1ZGlvV29ya2xldE5vZGVcIixcbiAgICAgICAgZGVwdGg6IDEsXG4gICAgICAgIGxvZ1NldHRpbmdzOiB7XG4gICAgICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIG5vbkV4aXN0aW5nUHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBleGNsdWRlZFByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgb3ZlcndyaXR0ZW5Qcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIGxvZ0NhbGxTdGFjazogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbnNBc1N0cmluZ3M6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25HZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHByZXZlbnRTZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHJlY3Vyc2l2ZTogZmFsc2UsXG4gICAgICAgICAgICBkZXB0aDogNSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHtcbiAgICAgICAgb2JqZWN0OiBcIkdhaW5Ob2RlXCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiR2Fpbk5vZGVcIixcbiAgICAgICAgZGVwdGg6IDAsXG4gICAgICAgIGxvZ1NldHRpbmdzOiB7XG4gICAgICAgICAgICBwcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIG5vbkV4aXN0aW5nUHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBleGNsdWRlZFByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgb3ZlcndyaXR0ZW5Qcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIGxvZ0NhbGxTdGFjazogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbnNBc1N0cmluZ3M6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25HZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHByZXZlbnRTZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHJlY3Vyc2l2ZTogZmFsc2UsXG4gICAgICAgICAgICBkZXB0aDogNSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHtcbiAgICAgICAgb2JqZWN0OiBcIkFuYWx5c2VyTm9kZVwiLFxuICAgICAgICBpbnN0cnVtZW50ZWROYW1lOiBcIkFuYWx5c2VyTm9kZVwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiT3NjaWxsYXRvck5vZGVcIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJPc2NpbGxhdG9yTm9kZVwiLFxuICAgICAgICBkZXB0aDogMSxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgLy8gQWRkIHNoYXJlZCBwcm90b3R5cGUgYnkgQW5hbHlzZXJOb2RlLCBPc2NpbGxhdG9yTm9kZSwgU2NyaXB0UHJvY2Vzc29yTm9kZSwgR2Fpbk5vZGUsIFNjcmlwdFByb2Nlc3Nvck5vZGVcbiAgICB7XG4gICAgICAgIG9iamVjdDogXCJBbmFseXNlck5vZGVcIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJOb2RlXCIsXG4gICAgICAgIGRlcHRoOiAxLFxuICAgICAgICBsb2dTZXR0aW5nczoge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIG92ZXJ3cml0dGVuUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBsb2dDYWxsU3RhY2s6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25zQXNTdHJpbmdzOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uR2V0czogZmFsc2UsXG4gICAgICAgICAgICBwcmV2ZW50U2V0czogZmFsc2UsXG4gICAgICAgICAgICByZWN1cnNpdmU6IGZhbHNlLFxuICAgICAgICAgICAgZGVwdGg6IDUsXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICB7XG4gICAgICAgIG9iamVjdDogXCJPZmZsaW5lQXVkaW9Db250ZXh0XCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiT2ZmbGluZUF1ZGlvQ29udGV4dFwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiQXVkaW9Db250ZXh0XCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiQXVkaW9Db250ZXh0XCIsXG4gICAgICAgIGRlcHRoOiAwLFxuICAgICAgICBsb2dTZXR0aW5nczoge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIG92ZXJ3cml0dGVuUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBsb2dDYWxsU3RhY2s6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25zQXNTdHJpbmdzOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uR2V0czogZmFsc2UsXG4gICAgICAgICAgICBwcmV2ZW50U2V0czogZmFsc2UsXG4gICAgICAgICAgICByZWN1cnNpdmU6IGZhbHNlLFxuICAgICAgICAgICAgZGVwdGg6IDUsXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICAvLyBBZGQgc2hhcmVkIHByb3RvdHlwZSBieSBBdWRpb0NvbnRlbnh0L09mZmxpbmVBdWRpb0NvbnRleHRcbiAgICB7XG4gICAgICAgIG9iamVjdDogXCJBdWRpb0NvbnRleHRcIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJbQXVkaW9Db250ZW54dHxPZmZsaW5lQXVkaW9Db250ZXh0XVwiLFxuICAgICAgICBkZXB0aDogMSxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiUlRDUGVlckNvbm5lY3Rpb25cIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJSVENQZWVyQ29ubmVjdGlvblwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiSFRNTENhbnZhc0VsZW1lbnRcIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJIVE1MQ2FudmFzRWxlbWVudFwiLFxuICAgICAgICBkZXB0aDogMSxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW1wic3R5bGVcIiwgXCJvZmZzZXRXaWR0aFwiLCBcIm9mZnNldEhlaWdodFwiXSxcbiAgICAgICAgICAgIG92ZXJ3cml0dGVuUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBsb2dDYWxsU3RhY2s6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25zQXNTdHJpbmdzOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uR2V0czogZmFsc2UsXG4gICAgICAgICAgICBwcmV2ZW50U2V0czogZmFsc2UsXG4gICAgICAgICAgICByZWN1cnNpdmU6IGZhbHNlLFxuICAgICAgICAgICAgZGVwdGg6IDUsXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICB7XG4gICAgICAgIG9iamVjdDogXCJTdG9yYWdlXCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiU3RvcmFnZVwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiTmF2aWdhdG9yXCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiTmF2aWdhdG9yXCIsXG4gICAgICAgIGRlcHRoOiAwLFxuICAgICAgICBsb2dTZXR0aW5nczoge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIG92ZXJ3cml0dGVuUHJvcGVydGllczogW3sga2V5OiBcIndlYmRyaXZlclwiLCB2YWx1ZTogZmFsc2UsIGxldmVsOiAwIH1dLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiQ2FudmFzUmVuZGVyaW5nQ29udGV4dDJEXCIsXG4gICAgICAgIGluc3RydW1lbnRlZE5hbWU6IFwiQ2FudmFzUmVuZGVyaW5nQ29udGV4dDJEXCIsXG4gICAgICAgIGRlcHRoOiAwLFxuICAgICAgICBsb2dTZXR0aW5nczoge1xuICAgICAgICAgICAgcHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBub25FeGlzdGluZ1Byb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgZXhjbHVkZWRQcm9wZXJ0aWVzOiBbXG4gICAgICAgICAgICAgICAgXCJ0cmFuc2Zvcm1cIixcbiAgICAgICAgICAgICAgICBcImdsb2JhbEFscGhhXCIsXG4gICAgICAgICAgICAgICAgXCJjbGVhclJlY3RcIixcbiAgICAgICAgICAgICAgICBcImNsb3NlUGF0aFwiLFxuICAgICAgICAgICAgICAgIFwiY2FudmFzXCIsXG4gICAgICAgICAgICAgICAgXCJxdWFkcmF0aWNDdXJ2ZVRvXCIsXG4gICAgICAgICAgICAgICAgXCJsaW5lVG9cIixcbiAgICAgICAgICAgICAgICBcIm1vdmVUb1wiLFxuICAgICAgICAgICAgICAgIFwic2V0VHJhbnNmb3JtXCIsXG4gICAgICAgICAgICAgICAgXCJkcmF3SW1hZ2VcIixcbiAgICAgICAgICAgICAgICBcImJlZ2luUGF0aFwiLFxuICAgICAgICAgICAgICAgIFwidHJhbnNsYXRlXCIsXG4gICAgICAgICAgICBdLFxuICAgICAgICAgICAgb3ZlcndyaXR0ZW5Qcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIGxvZ0NhbGxTdGFjazogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbnNBc1N0cmluZ3M6IGZhbHNlLFxuICAgICAgICAgICAgbG9nRnVuY3Rpb25HZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHByZXZlbnRTZXRzOiBmYWxzZSxcbiAgICAgICAgICAgIHJlY3Vyc2l2ZTogZmFsc2UsXG4gICAgICAgICAgICBkZXB0aDogNSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHtcbiAgICAgICAgb2JqZWN0OiBcIlNjcmVlblwiLFxuICAgICAgICBpbnN0cnVtZW50ZWROYW1lOiBcIlNjcmVlblwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFtdLFxuICAgICAgICAgICAgLy8gaW4gT3BlbldQTSBpcyBvbmx5IHRoaXMgb25lIHVzZWQ6XG4gICAgICAgICAgICAvLyB7XCJkZXB0aFwiOjAsIFwicHJvcGVydHlOYW1lc1wiOltcImNvbG9yRGVwdGhcIixcInBpeGVsRGVwdGhcIlxuICAgICAgICAgICAgbm9uRXhpc3RpbmdQcm9wZXJ0aWVzVG9JbnN0cnVtZW50OiBbXSxcbiAgICAgICAgICAgIGV4Y2x1ZGVkUHJvcGVydGllczogW10sXG4gICAgICAgICAgICBvdmVyd3JpdHRlblByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgbG9nQ2FsbFN0YWNrOiBmYWxzZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG4gICAge1xuICAgICAgICBvYmplY3Q6IFwiZG9jdW1lbnRcIixcbiAgICAgICAgaW5zdHJ1bWVudGVkTmFtZTogXCJkb2N1bWVudFwiLFxuICAgICAgICBkZXB0aDogMCxcbiAgICAgICAgbG9nU2V0dGluZ3M6IHtcbiAgICAgICAgICAgIHByb3BlcnRpZXNUb0luc3RydW1lbnQ6IFt7IGRlcHRoOiAyLCBwcm9wZXJ0eU5hbWVzOiBbXCJyZWZlcnJlclwiXSB9XSxcbiAgICAgICAgICAgIG5vbkV4aXN0aW5nUHJvcGVydGllc1RvSW5zdHJ1bWVudDogW10sXG4gICAgICAgICAgICBleGNsdWRlZFByb3BlcnRpZXM6IFtdLFxuICAgICAgICAgICAgb3ZlcndyaXR0ZW5Qcm9wZXJ0aWVzOiBbXSxcbiAgICAgICAgICAgIGxvZ0NhbGxTdGFjazogdHJ1ZSxcbiAgICAgICAgICAgIGxvZ0Z1bmN0aW9uc0FzU3RyaW5nczogZmFsc2UsXG4gICAgICAgICAgICBsb2dGdW5jdGlvbkdldHM6IGZhbHNlLFxuICAgICAgICAgICAgcHJldmVudFNldHM6IGZhbHNlLFxuICAgICAgICAgICAgcmVjdXJzaXZlOiBmYWxzZSxcbiAgICAgICAgICAgIGRlcHRoOiA1LFxuICAgICAgICB9LFxuICAgIH0sXG5dO1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCxleUoyWlhKemFXOXVJam96TENKbWFXeGxJam9pYzJWMGRHbHVaM011YW5NaUxDSnpiM1Z5WTJWU2IyOTBJam9pSWl3aWMyOTFjbU5sY3lJNld5SXVMaTh1TGk4dUxpOXpjbU12YzNSbFlXeDBhQzl6WlhSMGFXNW5jeTUwY3lKZExDSnVZVzFsY3lJNlcxMHNJbTFoY0hCcGJtZHpJam9pT3pzN1FVRkZZU3hSUVVGQkxIbENRVUY1UWl4SFFVRjVRanRKUVVNM1JEdFJRVU5GTEUxQlFVMHNSVUZCUlN4eFFrRkJjVUlzUlVGQlJTdzBRMEZCTkVNN1VVRkRNMFVzWjBKQlFXZENMRVZCUVVVc2NVSkJRWEZDTzFGQlEzWkRMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMUlzVjBGQlZ5eEZRVUZGTzFsQlExZ3NjMEpCUVhOQ0xFVkJRVVVzUlVGQlJUdFpRVU14UWl4cFEwRkJhVU1zUlVGQlJTeEZRVUZGTzFsQlEzSkRMR3RDUVVGclFpeEZRVUZGTEVWQlFVVTdXVUZEZEVJc2NVSkJRWEZDTEVWQlFVVXNSVUZCUlR0WlFVTjZRaXhaUVVGWkxFVkJRVVVzUzBGQlN6dFpRVU51UWl4eFFrRkJjVUlzUlVGQlJTeExRVUZMTzFsQlF6VkNMR1ZCUVdVc1JVRkJSU3hMUVVGTE8xbEJRM1JDTEZkQlFWY3NSVUZCUlN4TFFVRkxPMWxCUTJ4Q0xGTkJRVk1zUlVGQlJTeExRVUZMTzFsQlEyaENMRXRCUVVzc1JVRkJSU3hEUVVGRE8xTkJRMVE3UzBGRFJqdEpRVVZFTzFGQlEwVXNUVUZCVFN4RlFVRkZMR3RDUVVGclFqdFJRVU14UWl4blFrRkJaMElzUlVGQlJTeHJRa0ZCYTBJN1VVRkRjRU1zUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVWl4WFFVRlhMRVZCUVVVN1dVRkRXQ3h6UWtGQmMwSXNSVUZCUlN4RlFVRkZPMWxCUXpGQ0xHbERRVUZwUXl4RlFVRkZMRVZCUVVVN1dVRkRja01zYTBKQlFXdENMRVZCUVVVc1JVRkJSVHRaUVVOMFFpeHhRa0ZCY1VJc1JVRkJSU3hGUVVGRk8xbEJRM3BDTEZsQlFWa3NSVUZCUlN4TFFVRkxPMWxCUTI1Q0xIRkNRVUZ4UWl4RlFVRkZMRXRCUVVzN1dVRkROVUlzWlVGQlpTeEZRVUZGTEV0QlFVczdXVUZEZEVJc1YwRkJWeXhGUVVGRkxFdEJRVXM3V1VGRGJFSXNVMEZCVXl4RlFVRkZMRXRCUVVzN1dVRkRhRUlzUzBGQlN5eEZRVUZGTEVOQlFVTTdVMEZEVkR0TFFVTkdPMGxCUlVRN1VVRkRSU3hOUVVGTkxFVkJRVVVzVlVGQlZUdFJRVU5zUWl4blFrRkJaMElzUlVGQlJTeFZRVUZWTzFGQlF6VkNMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMUlzVjBGQlZ5eEZRVUZGTzFsQlExZ3NjMEpCUVhOQ0xFVkJRVVVzUlVGQlJUdFpRVU14UWl4cFEwRkJhVU1zUlVGQlJTeEZRVUZGTzFsQlEzSkRMR3RDUVVGclFpeEZRVUZGTEVWQlFVVTdXVUZEZEVJc2NVSkJRWEZDTEVWQlFVVXNSVUZCUlR0WlFVTjZRaXhaUVVGWkxFVkJRVVVzUzBGQlN6dFpRVU51UWl4eFFrRkJjVUlzUlVGQlJTeExRVUZMTzFsQlF6VkNMR1ZCUVdVc1JVRkJSU3hMUVVGTE8xbEJRM1JDTEZkQlFWY3NSVUZCUlN4TFFVRkxPMWxCUTJ4Q0xGTkJRVk1zUlVGQlJTeExRVUZMTzFsQlEyaENMRXRCUVVzc1JVRkJSU3hEUVVGRE8xTkJRMVE3UzBGRFJqdEpRVVZFTzFGQlEwVXNUVUZCVFN4RlFVRkZMR05CUVdNN1VVRkRkRUlzWjBKQlFXZENMRVZCUVVVc1kwRkJZenRSUVVOb1F5eExRVUZMTEVWQlFVVXNRMEZCUXp0UlFVTlNMRmRCUVZjc1JVRkJSVHRaUVVOWUxITkNRVUZ6UWl4RlFVRkZMRVZCUVVVN1dVRkRNVUlzYVVOQlFXbERMRVZCUVVVc1JVRkJSVHRaUVVOeVF5eHJRa0ZCYTBJc1JVRkJSU3hGUVVGRk8xbEJRM1JDTEhGQ1FVRnhRaXhGUVVGRkxFVkJRVVU3V1VGRGVrSXNXVUZCV1N4RlFVRkZMRXRCUVVzN1dVRkRia0lzY1VKQlFYRkNMRVZCUVVVc1MwRkJTenRaUVVNMVFpeGxRVUZsTEVWQlFVVXNTMEZCU3p0WlFVTjBRaXhYUVVGWExFVkJRVVVzUzBGQlN6dFpRVU5zUWl4VFFVRlRMRVZCUVVVc1MwRkJTenRaUVVOb1FpeExRVUZMTEVWQlFVVXNRMEZCUXp0VFFVTlVPMHRCUTBZN1NVRkZSRHRSUVVORkxFMUJRVTBzUlVGQlJTeG5Ra0ZCWjBJN1VVRkRlRUlzWjBKQlFXZENMRVZCUVVVc1owSkJRV2RDTzFGQlEyeERMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMUlzVjBGQlZ5eEZRVUZGTzFsQlExZ3NjMEpCUVhOQ0xFVkJRVVVzUlVGQlJUdFpRVU14UWl4cFEwRkJhVU1zUlVGQlJTeEZRVUZGTzFsQlEzSkRMR3RDUVVGclFpeEZRVUZGTEVWQlFVVTdXVUZEZEVJc2NVSkJRWEZDTEVWQlFVVXNSVUZCUlR0WlFVTjZRaXhaUVVGWkxFVkJRVVVzUzBGQlN6dFpRVU51UWl4eFFrRkJjVUlzUlVGQlJTeExRVUZMTzFsQlF6VkNMR1ZCUVdVc1JVRkJSU3hMUVVGTE8xbEJRM1JDTEZkQlFWY3NSVUZCUlN4TFFVRkxPMWxCUTJ4Q0xGTkJRVk1zUlVGQlJTeExRVUZMTzFsQlEyaENMRXRCUVVzc1JVRkJSU3hEUVVGRE8xTkJRMVE3UzBGRFJqdEpRVVZFTERKSFFVRXlSenRKUVVNelJ6dFJRVU5GTEUxQlFVMHNSVUZCUlN4alFVRmpPMUZCUTNSQ0xHZENRVUZuUWl4RlFVRkZMRTFCUVUwN1VVRkRlRUlzUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVWl4WFFVRlhMRVZCUVVVN1dVRkRXQ3h6UWtGQmMwSXNSVUZCUlN4RlFVRkZPMWxCUXpGQ0xHbERRVUZwUXl4RlFVRkZMRVZCUVVVN1dVRkRja01zYTBKQlFXdENMRVZCUVVVc1JVRkJSVHRaUVVOMFFpeHhRa0ZCY1VJc1JVRkJSU3hGUVVGRk8xbEJRM3BDTEZsQlFWa3NSVUZCUlN4TFFVRkxPMWxCUTI1Q0xIRkNRVUZ4UWl4RlFVRkZMRXRCUVVzN1dVRkROVUlzWlVGQlpTeEZRVUZGTEV0QlFVczdXVUZEZEVJc1YwRkJWeXhGUVVGRkxFdEJRVXM3V1VGRGJFSXNVMEZCVXl4RlFVRkZMRXRCUVVzN1dVRkRhRUlzUzBGQlN5eEZRVUZGTEVOQlFVTTdVMEZEVkR0TFFVTkdPMGxCUlVRN1VVRkRSU3hOUVVGTkxFVkJRVVVzY1VKQlFYRkNPMUZCUXpkQ0xHZENRVUZuUWl4RlFVRkZMSEZDUVVGeFFqdFJRVU4yUXl4TFFVRkxMRVZCUVVVc1EwRkJRenRSUVVOU0xGZEJRVmNzUlVGQlJUdFpRVU5ZTEhOQ1FVRnpRaXhGUVVGRkxFVkJRVVU3V1VGRE1VSXNhVU5CUVdsRExFVkJRVVVzUlVGQlJUdFpRVU55UXl4clFrRkJhMElzUlVGQlJTeEZRVUZGTzFsQlEzUkNMSEZDUVVGeFFpeEZRVUZGTEVWQlFVVTdXVUZEZWtJc1dVRkJXU3hGUVVGRkxFdEJRVXM3V1VGRGJrSXNjVUpCUVhGQ0xFVkJRVVVzUzBGQlN6dFpRVU0xUWl4bFFVRmxMRVZCUVVVc1MwRkJTenRaUVVOMFFpeFhRVUZYTEVWQlFVVXNTMEZCU3p0WlFVTnNRaXhUUVVGVExFVkJRVVVzUzBGQlN6dFpRVU5vUWl4TFFVRkxMRVZCUVVVc1EwRkJRenRUUVVOVU8wdEJRMFk3U1VGRlJEdFJRVU5GTEUxQlFVMHNSVUZCUlN4alFVRmpPMUZCUTNSQ0xHZENRVUZuUWl4RlFVRkZMR05CUVdNN1VVRkRhRU1zUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVWl4WFFVRlhMRVZCUVVVN1dVRkRXQ3h6UWtGQmMwSXNSVUZCUlN4RlFVRkZPMWxCUXpGQ0xHbERRVUZwUXl4RlFVRkZMRVZCUVVVN1dVRkRja01zYTBKQlFXdENMRVZCUVVVc1JVRkJSVHRaUVVOMFFpeHhRa0ZCY1VJc1JVRkJSU3hGUVVGRk8xbEJRM3BDTEZsQlFWa3NSVUZCUlN4TFFVRkxPMWxCUTI1Q0xIRkNRVUZ4UWl4RlFVRkZMRXRCUVVzN1dVRkROVUlzWlVGQlpTeEZRVUZGTEV0QlFVczdXVUZEZEVJc1YwRkJWeXhGUVVGRkxFdEJRVXM3V1VGRGJFSXNVMEZCVXl4RlFVRkZMRXRCUVVzN1dVRkRhRUlzUzBGQlN5eEZRVUZGTEVOQlFVTTdVMEZEVkR0TFFVTkdPMGxCUlVRc05FUkJRVFJFTzBsQlF6VkVPMUZCUTBVc1RVRkJUU3hGUVVGRkxHTkJRV003VVVGRGRFSXNaMEpCUVdkQ0xFVkJRVVVzY1VOQlFYRkRPMUZCUTNaRUxFdEJRVXNzUlVGQlJTeERRVUZETzFGQlExSXNWMEZCVnl4RlFVRkZPMWxCUTFnc2MwSkJRWE5DTEVWQlFVVXNSVUZCUlR0WlFVTXhRaXhwUTBGQmFVTXNSVUZCUlN4RlFVRkZPMWxCUTNKRExHdENRVUZyUWl4RlFVRkZMRVZCUVVVN1dVRkRkRUlzY1VKQlFYRkNMRVZCUVVVc1JVRkJSVHRaUVVONlFpeFpRVUZaTEVWQlFVVXNTMEZCU3p0WlFVTnVRaXh4UWtGQmNVSXNSVUZCUlN4TFFVRkxPMWxCUXpWQ0xHVkJRV1VzUlVGQlJTeExRVUZMTzFsQlEzUkNMRmRCUVZjc1JVRkJSU3hMUVVGTE8xbEJRMnhDTEZOQlFWTXNSVUZCUlN4TFFVRkxPMWxCUTJoQ0xFdEJRVXNzUlVGQlJTeERRVUZETzFOQlExUTdTMEZEUmp0SlFVVkVPMUZCUTBVc1RVRkJUU3hGUVVGRkxHMUNRVUZ0UWp0UlFVTXpRaXhuUWtGQlowSXNSVUZCUlN4dFFrRkJiVUk3VVVGRGNrTXNTMEZCU3l4RlFVRkZMRU5CUVVNN1VVRkRVaXhYUVVGWExFVkJRVVU3V1VGRFdDeHpRa0ZCYzBJc1JVRkJSU3hGUVVGRk8xbEJRekZDTEdsRFFVRnBReXhGUVVGRkxFVkJRVVU3V1VGRGNrTXNhMEpCUVd0Q0xFVkJRVVVzUlVGQlJUdFpRVU4wUWl4eFFrRkJjVUlzUlVGQlJTeEZRVUZGTzFsQlEzcENMRmxCUVZrc1JVRkJSU3hMUVVGTE8xbEJRMjVDTEhGQ1FVRnhRaXhGUVVGRkxFdEJRVXM3V1VGRE5VSXNaVUZCWlN4RlFVRkZMRXRCUVVzN1dVRkRkRUlzVjBGQlZ5eEZRVUZGTEV0QlFVczdXVUZEYkVJc1UwRkJVeXhGUVVGRkxFdEJRVXM3V1VGRGFFSXNTMEZCU3l4RlFVRkZMRU5CUVVNN1UwRkRWRHRMUVVOR08wbEJSVVE3VVVGRFJTeE5RVUZOTEVWQlFVVXNiVUpCUVcxQ08xRkJRek5DTEdkQ1FVRm5RaXhGUVVGRkxHMUNRVUZ0UWp0UlFVTnlReXhMUVVGTExFVkJRVVVzUTBGQlF6dFJRVU5TTEZkQlFWY3NSVUZCUlR0WlFVTllMSE5DUVVGelFpeEZRVUZGTEVWQlFVVTdXVUZETVVJc2FVTkJRV2xETEVWQlFVVXNSVUZCUlR0WlFVTnlReXhyUWtGQmEwSXNSVUZCUlN4RFFVRkRMRTlCUVU4c1JVRkJSU3hoUVVGaExFVkJRVVVzWTBGQll5eERRVUZETzFsQlF6VkVMSEZDUVVGeFFpeEZRVUZGTEVWQlFVVTdXVUZEZWtJc1dVRkJXU3hGUVVGRkxFdEJRVXM3V1VGRGJrSXNjVUpCUVhGQ0xFVkJRVVVzUzBGQlN6dFpRVU0xUWl4bFFVRmxMRVZCUVVVc1MwRkJTenRaUVVOMFFpeFhRVUZYTEVWQlFVVXNTMEZCU3p0WlFVTnNRaXhUUVVGVExFVkJRVVVzUzBGQlN6dFpRVU5vUWl4TFFVRkxMRVZCUVVVc1EwRkJRenRUUVVOVU8wdEJRMFk3U1VGRlJEdFJRVU5GTEUxQlFVMHNSVUZCUlN4VFFVRlRPMUZCUTJwQ0xHZENRVUZuUWl4RlFVRkZMRk5CUVZNN1VVRkRNMElzUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEVWl4WFFVRlhMRVZCUVVVN1dVRkRXQ3h6UWtGQmMwSXNSVUZCUlN4RlFVRkZPMWxCUXpGQ0xHbERRVUZwUXl4RlFVRkZMRVZCUVVVN1dVRkRja01zYTBKQlFXdENMRVZCUVVVc1JVRkJSVHRaUVVOMFFpeHhRa0ZCY1VJc1JVRkJSU3hGUVVGRk8xbEJRM3BDTEZsQlFWa3NSVUZCUlN4TFFVRkxPMWxCUTI1Q0xIRkNRVUZ4UWl4RlFVRkZMRXRCUVVzN1dVRkROVUlzWlVGQlpTeEZRVUZGTEV0QlFVczdXVUZEZEVJc1YwRkJWeXhGUVVGRkxFdEJRVXM3V1VGRGJFSXNVMEZCVXl4RlFVRkZMRXRCUVVzN1dVRkRhRUlzUzBGQlN5eEZRVUZGTEVOQlFVTTdVMEZEVkR0TFFVTkdPMGxCUlVRN1VVRkRSU3hOUVVGTkxFVkJRVVVzVjBGQlZ6dFJRVU51UWl4blFrRkJaMElzUlVGQlJTeFhRVUZYTzFGQlF6ZENMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMUlzVjBGQlZ5eEZRVUZGTzFsQlExZ3NjMEpCUVhOQ0xFVkJRVVVzUlVGQlJUdFpRVU14UWl4cFEwRkJhVU1zUlVGQlJTeEZRVUZGTzFsQlEzSkRMR3RDUVVGclFpeEZRVUZGTEVWQlFVVTdXVUZEZEVJc2NVSkJRWEZDTEVWQlFVVXNRMEZCUXl4RlFVRkZMRWRCUVVjc1JVRkJSU3hYUVVGWExFVkJRVVVzUzBGQlN5eEZRVUZGTEV0QlFVc3NSVUZCUlN4TFFVRkxMRVZCUVVVc1EwRkJReXhGUVVGRkxFTkJRVU03V1VGRGNrVXNXVUZCV1N4RlFVRkZMRXRCUVVzN1dVRkRia0lzY1VKQlFYRkNMRVZCUVVVc1MwRkJTenRaUVVNMVFpeGxRVUZsTEVWQlFVVXNTMEZCU3p0WlFVTjBRaXhYUVVGWExFVkJRVVVzUzBGQlN6dFpRVU5zUWl4VFFVRlRMRVZCUVVVc1MwRkJTenRaUVVOb1FpeExRVUZMTEVWQlFVVXNRMEZCUXp0VFFVTlVPMHRCUTBZN1NVRkZSRHRSUVVORkxFMUJRVTBzUlVGQlJTd3dRa0ZCTUVJN1VVRkRiRU1zWjBKQlFXZENMRVZCUVVVc01FSkJRVEJDTzFGQlF6VkRMRXRCUVVzc1JVRkJSU3hEUVVGRE8xRkJRMUlzVjBGQlZ5eEZRVUZGTzFsQlExZ3NjMEpCUVhOQ0xFVkJRVVVzUlVGQlJUdFpRVU14UWl4cFEwRkJhVU1zUlVGQlJTeEZRVUZGTzFsQlEzSkRMR3RDUVVGclFpeEZRVUZGTzJkQ1FVTnNRaXhYUVVGWE8yZENRVU5ZTEdGQlFXRTdaMEpCUTJJc1YwRkJWenRuUWtGRFdDeFhRVUZYTzJkQ1FVTllMRkZCUVZFN1owSkJRMUlzYTBKQlFXdENPMmRDUVVOc1FpeFJRVUZSTzJkQ1FVTlNMRkZCUVZFN1owSkJRMUlzWTBGQll6dG5Ra0ZEWkN4WFFVRlhPMmRDUVVOWUxGZEJRVmM3WjBKQlExZ3NWMEZCVnp0aFFVTmFPMWxCUTBRc2NVSkJRWEZDTEVWQlFVVXNSVUZCUlR0WlFVTjZRaXhaUVVGWkxFVkJRVVVzUzBGQlN6dFpRVU51UWl4eFFrRkJjVUlzUlVGQlJTeExRVUZMTzFsQlF6VkNMR1ZCUVdVc1JVRkJSU3hMUVVGTE8xbEJRM1JDTEZkQlFWY3NSVUZCUlN4TFFVRkxPMWxCUTJ4Q0xGTkJRVk1zUlVGQlJTeExRVUZMTzFsQlEyaENMRXRCUVVzc1JVRkJSU3hEUVVGRE8xTkJRMVE3UzBGRFJqdEpRVVZFTzFGQlEwVXNUVUZCVFN4RlFVRkZMRkZCUVZFN1VVRkRhRUlzWjBKQlFXZENMRVZCUVVVc1VVRkJVVHRSUVVNeFFpeExRVUZMTEVWQlFVVXNRMEZCUXp0UlFVTlNMRmRCUVZjc1JVRkJSVHRaUVVOWUxITkNRVUZ6UWl4RlFVRkZMRVZCUVVVN1dVRkRNVUlzYjBOQlFXOURPMWxCUTNCRExIbEVRVUY1UkR0WlFVTjZSQ3hwUTBGQmFVTXNSVUZCUlN4RlFVRkZPMWxCUTNKRExHdENRVUZyUWl4RlFVRkZMRVZCUVVVN1dVRkRkRUlzY1VKQlFYRkNMRVZCUVVVc1JVRkJSVHRaUVVONlFpeFpRVUZaTEVWQlFVVXNTMEZCU3p0WlFVTnVRaXh4UWtGQmNVSXNSVUZCUlN4TFFVRkxPMWxCUXpWQ0xHVkJRV1VzUlVGQlJTeExRVUZMTzFsQlEzUkNMRmRCUVZjc1JVRkJSU3hMUVVGTE8xbEJRMnhDTEZOQlFWTXNSVUZCUlN4TFFVRkxPMWxCUTJoQ0xFdEJRVXNzUlVGQlJTeERRVUZETzFOQlExUTdTMEZEUmp0SlFVVkVPMUZCUTBVc1RVRkJUU3hGUVVGRkxGVkJRVlU3VVVGRGJFSXNaMEpCUVdkQ0xFVkJRVVVzVlVGQlZUdFJRVU0xUWl4TFFVRkxMRVZCUVVVc1EwRkJRenRSUVVOU0xGZEJRVmNzUlVGQlJUdFpRVU5ZTEhOQ1FVRnpRaXhGUVVGRkxFTkJRVU1zUlVGQlJTeExRVUZMTEVWQlFVVXNRMEZCUXl4RlFVRkZMR0ZCUVdFc1JVRkJSU3hEUVVGRExGVkJRVlVzUTBGQlF5eEZRVUZGTEVOQlFVTTdXVUZEYmtVc2FVTkJRV2xETEVWQlFVVXNSVUZCUlR0WlFVTnlReXhyUWtGQmEwSXNSVUZCUlN4RlFVRkZPMWxCUTNSQ0xIRkNRVUZ4UWl4RlFVRkZMRVZCUVVVN1dVRkRla0lzV1VGQldTeEZRVUZGTEVsQlFVazdXVUZEYkVJc2NVSkJRWEZDTEVWQlFVVXNTMEZCU3p0WlFVTTFRaXhsUVVGbExFVkJRVVVzUzBGQlN6dFpRVU4wUWl4WFFVRlhMRVZCUVVVc1MwRkJTenRaUVVOc1FpeFRRVUZUTEVWQlFVVXNTMEZCU3p0WlFVTm9RaXhMUVVGTExFVkJRVVVzUTBGQlF6dFRRVU5VTzB0QlEwWTdRMEZEUml4RFFVRkRJbjA9IiwiXCJ1c2Ugc3RyaWN0XCI7XG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHsgdmFsdWU6IHRydWUgfSk7XG4vKiBUYWtlbiBmcm9tIGh0dHBzOi8vZ2l0aHViLmNvbS9ra2Fwc25lci9DYW52YXNCbG9ja2VyIHdpdGggc21hbGwgY2hhbmdlc1xuICogVGhpcyBTb3VyY2UgQ29kZSBGb3JtIGlzIHN1YmplY3QgdG8gdGhlIHRlcm1zIG9mIHRoZSBNb3ppbGxhIFB1YmxpY1xuICogTGljZW5zZSwgdi4gMi4wLiBJZiBhIGNvcHkgb2YgdGhlIE1QTCB3YXMgbm90IGRpc3RyaWJ1dGVkIHdpdGggdGhpc1xuICogZmlsZSwgWW91IGNhbiBvYnRhaW4gb25lIGF0IGh0dHA6Ly9tb3ppbGxhLm9yZy9NUEwvMi4wLy4gKi9cbmNvbnN0IGluc3RydW1lbnRfMSA9IHJlcXVpcmUoXCIuL2luc3RydW1lbnRcIik7XG4vLyBEZWNsYXJpbmcgc29tZSBsb2NhbCB0cmFja2Vyc1xuY29uc3QgaW50ZXJjZXB0ZWRXaW5kb3dzID0gbmV3IFdlYWtNYXAoKTtcbmNvbnN0IHByb3hpZXMgPSBuZXcgTWFwKCk7XG5jb25zdCBjaGFuZ2VkVG9TdHJpbmdzID0gbmV3IFdlYWtNYXAoKTtcbi8vIEVudHJ5IHBvaW50IGZvciB0aGlzIGV4dGVuc2lvblxuKGZ1bmN0aW9uICgpIHtcbiAgICAvLyBjb25zb2xlLmxvZyhcIlN0YXJ0aW5nIGZyYW1lIHNjcmlwdFwiKTtcbiAgICB0cnkge1xuICAgICAgICBpbnRlcmNlcHRXaW5kb3cod2luZG93KTtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFwiSW5zdHJ1bWVudGF0aW9uIGluaXRpYWxpc2F0aW9uIGNyYXNoZWQuIFJlYXNvbjogXCIgKyBlcnJvcik7XG4gICAgICAgIGNvbnNvbGUubG9nKGVycm9yLnN0YWNrKTtcbiAgICB9XG4gICAgLy8gY29uc29sZS5sb2coXCJTdGFydGluZyBmcmFtZSBzY3JpcHRcIik7XG59KSgpO1xuZnVuY3Rpb24gaW50ZXJjZXB0V2luZG93KGNvbnRleHQpIHtcbiAgICBsZXQgd3JhcHBlZFRyeTtcbiAgICB0cnkge1xuICAgICAgICB3cmFwcGVkVHJ5ID0gZ2V0V3JhcHBlZChjb250ZXh0KTtcbiAgICB9XG4gICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIC8vIHdlIGFyZSB1bmFibGUgdG8gcmVhZCB0aGUgbG9jYXRpb24gZHVlIHRvIFNPUFxuICAgICAgICAvLyB0aGVyZWZvcmUgd2UgYWxzbyBjYW4gbm90IGludGVyY2VwdCBhbnl0aGluZy5cbiAgICAgICAgLy8gY29uc29sZS5sb2coXCJOT1QgaW50ZXJjZXB0aW5nIHdpbmRvdyBkdWUgdG8gU09QOiBcIiwgY29udGV4dCk7XG4gICAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgY29uc3Qgd3JhcHBlZFdpbmRvdyA9IHdyYXBwZWRUcnk7XG4gICAgaWYgKGludGVyY2VwdGVkV2luZG93cy5nZXQod3JhcHBlZFdpbmRvdykpIHtcbiAgICAgICAgLy8gY29uc29sZS5sb2coXCJBbHJlYWR5IGludGVyY2VwdGVkOiBcIiwgY29udGV4dCk7XG4gICAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgLy8gY29uc29sZS5sb2coXCJpbnRlcmNlcHRpbmcgd2luZG93XCIsIGNvbnRleHQpO1xuICAgICgwLCBpbnN0cnVtZW50XzEuc3RhcnRJbnN0cnVtZW50KShjb250ZXh0KTtcbiAgICBpbnRlcmNlcHRlZFdpbmRvd3Muc2V0KHdyYXBwZWRXaW5kb3csIHRydWUpO1xuICAgIC8vIGNvbnNvbGUubG9nKFwicHJlcGFyZSB0byBpbnRlcmNlcHQgXCIrIGNvbnRleHQubGVuZ3RoICtcIiAoaSlmcmFtZXMuXCIpO1xuICAgIGZ1bmN0aW9uIGludGVyY2VwdEFsbEZyYW1lcygpIHtcbiAgICAgICAgY29uc3QgY3VycmVudExlbmd0aCA9IGNvbnRleHQubGVuZ3RoO1xuICAgICAgICBmb3IgKGxldCBpID0gY3VycmVudExlbmd0aDsgaS0tOykge1xuICAgICAgICAgICAgaWYgKCFpbnRlcmNlcHRlZFdpbmRvd3MuZ2V0KHdyYXBwZWRXaW5kb3dbaV0pKSB7XG4gICAgICAgICAgICAgICAgaW50ZXJjZXB0V2luZG93KGNvbnRleHRbaV0pO1xuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgfVxuICAgIHByb3RlY3RBbGxGcmFtZXMoY29udGV4dCwgd3JhcHBlZFdpbmRvdywgaW50ZXJjZXB0V2luZG93LCBpbnRlcmNlcHRBbGxGcmFtZXMpO1xuICAgIHJldHVybiB0cnVlO1xufVxuZnVuY3Rpb24gcHJvdGVjdEFsbEZyYW1lcyhjb250ZXh0LCB3cmFwcGVkV2luZG93LCBzaW5nbGVDYWxsYmFjaywgYWxsQ2FsbGJhY2spIHtcbiAgICBjb25zdCBjaGFuZ2VXaW5kb3dQcm9wZXJ0eSA9IGNyZWF0ZUNoYW5nZVByb3BlcnR5KGNvbnRleHQpO1xuICAgIGlmICghY2hhbmdlV2luZG93UHJvcGVydHkpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBhcGkgPSB7XG4gICAgICAgIGNvbnRleHQsXG4gICAgICAgIHdyYXBwZWRXaW5kb3csXG4gICAgICAgIGNoYW5nZVdpbmRvd1Byb3BlcnR5LFxuICAgICAgICBzaW5nbGVDYWxsYmFjayxcbiAgICAgICAgYWxsQ2FsbGJhY2ssXG4gICAgICAgIG9ic2VydmU6IG51bGwsXG4gICAgfTtcbiAgICBwcm90ZWN0RnJhbWVQcm9wZXJ0aWVzKGFwaSk7XG4gICAgcHJvdGVjdERPTU1vZGlmaWNhdGlvbnMoYXBpKTtcbiAgICAvLyBNdXRhdGlvbk9ic2VydmVyIHRvIGludGVyY2VwdCBpRnJhbWVzIHdoaWxlIGdlbmVyYXRpbmcgdGhlIERPTS5cbiAgICBhcGkub2JzZXJ2ZSA9IGVuYWJsZU11dGF0aW9uT2JzZXJ2ZXIoYXBpKTtcbiAgICAvLyBNdXRhdGlvbk9ic2VydmVyIGRvZXMgbm90IHRyaWdnZXIgZmFzdCBlbm91Z2ggd2hlbiBkb2N1bWVudC53cml0ZSBpcyB1c2VkXG4gICAgcHJvdGVjdERvY3VtZW50V3JpdGUoYXBpKTtcbiAgICBwcm90ZWN0V2luZG93T3BlbihhcGkpO1xufVxuZnVuY3Rpb24gZ2V0V3JhcHBlZChjb250ZXh0KSB7XG4gICAgcmV0dXJuIGNvbnRleHQgJiYgKGNvbnRleHQud3JhcHBlZEpTT2JqZWN0IHx8IGNvbnRleHQpO1xufVxuZnVuY3Rpb24gY3JlYXRlQ2hhbmdlUHJvcGVydHkod2luZG93KSB7XG4gICAgY29uc3QgY2hhbmdlV2luZG93UHJvcGVydHkgPSBmdW5jdGlvbiAob2JqZWN0LCBuYW1lLCB0eXBlLCBjaGFuZ2VkKSB7XG4gICAgICAgIGNvbnN0IGRlc2NyaXB0b3IgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKG9iamVjdCwgbmFtZSk7XG4gICAgICAgIGNvbnN0IG9yaWdpbmFsID0gZGVzY3JpcHRvclt0eXBlXTtcbiAgICAgICAgaWYgKHR5cGVvZiBjaGFuZ2VkID09PSBcImZ1bmN0aW9uXCIpIHtcbiAgICAgICAgICAgIGNoYW5nZWQgPSBjcmVhdGVQcm94eUZ1bmN0aW9uKHdpbmRvdywgb3JpZ2luYWwsIGNoYW5nZWQpO1xuICAgICAgICB9XG4gICAgICAgIGNoYW5nZVByb3BlcnR5RnVuYyh3aW5kb3csIHsgb2JqZWN0LCBuYW1lLCB0eXBlLCBjaGFuZ2VkIH0pO1xuICAgIH07XG4gICAgcmV0dXJuIGNoYW5nZVdpbmRvd1Byb3BlcnR5O1xufVxuZnVuY3Rpb24gY3JlYXRlUHJveHlGdW5jdGlvbihjb250ZXh0LCBvcmlnaW5hbCwgcmVwbGFjZW1lbnQpIHtcbiAgICBpZiAoIWNoYW5nZWRUb1N0cmluZ3MuZ2V0KGNvbnRleHQpKSB7XG4gICAgICAgIGNoYW5nZWRUb1N0cmluZ3Muc2V0KGNvbnRleHQsIHRydWUpO1xuICAgICAgICBjb25zdCBmdW5jdGlvblByb3RvdHlwZSA9IGdldFdyYXBwZWQoY29udGV4dCkuRnVuY3Rpb24ucHJvdG90eXBlO1xuICAgICAgICBjb25zdCB0b1N0cmluZyA9IGZ1bmN0aW9uUHJvdG90eXBlLnRvU3RyaW5nO1xuICAgICAgICBjaGFuZ2VQcm9wZXJ0eUZ1bmMoY29udGV4dCwge1xuICAgICAgICAgICAgb2JqZWN0OiBmdW5jdGlvblByb3RvdHlwZSxcbiAgICAgICAgICAgIG5hbWU6IFwidG9TdHJpbmdcIixcbiAgICAgICAgICAgIHR5cGU6IFwidmFsdWVcIixcbiAgICAgICAgICAgIGNoYW5nZWQ6IGNyZWF0ZVByb3h5RnVuY3Rpb24oY29udGV4dCwgdG9TdHJpbmcsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gcHJveGllcy5nZXQodGhpcykgfHwgdG9TdHJpbmcuY2FsbCh0aGlzKTtcbiAgICAgICAgICAgIH0pLFxuICAgICAgICB9KTtcbiAgICB9XG4gICAgY29uc3QgaGFuZGxlciA9IGdldFdyYXBwZWQoY29udGV4dCkuT2JqZWN0LmNyZWF0ZShudWxsKTtcbiAgICBoYW5kbGVyLmFwcGx5ID0gKDAsIGluc3RydW1lbnRfMS5leHBvcnRDdXN0b21GdW5jdGlvbikoZnVuY3Rpb24gKHRhcmdldCwgdGhpc0FyZ3MsIGFyZ3MpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIHJldHVybiBhcmdzLmxlbmd0aFxuICAgICAgICAgICAgICAgID8gcmVwbGFjZW1lbnQuY2FsbCh0aGlzQXJncywgLi4uYXJncylcbiAgICAgICAgICAgICAgICA6IHJlcGxhY2VtZW50LmNhbGwodGhpc0FyZ3MpO1xuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgICAgICByZXR1cm4gb3JpZ2luYWwuYXBwbHkodGhpc0FyZ3MsIGFyZ3MpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHRhcmdldC5hcHBseSh0aGlzQXJncywgYXJncyk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICB9LCBjb250ZXh0LCBcIlwiKTtcbiAgICBjb25zdCBwcm94eSA9IG5ldyBjb250ZXh0LlByb3h5KG9yaWdpbmFsLCBoYW5kbGVyKTtcbiAgICBwcm94aWVzLnNldChwcm94eSwgb3JpZ2luYWwudG9TdHJpbmcoKSk7XG4gICAgcmV0dXJuIGdldFdyYXBwZWQocHJveHkpO1xufVxuZnVuY3Rpb24gY2hhbmdlUHJvcGVydHlGdW5jKF9jb250ZXh0LCB7IG9iamVjdCwgbmFtZSwgdHlwZSwgY2hhbmdlZCB9KSB7XG4gICAgLy8gUmVtb3ZlZCB0cmFja2VyIGZvciBjaGFuZ2VkIHByb3BlcnRpZXNcbiAgICBjb25zdCBkZXNjcmlwdG9yID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihvYmplY3QsIG5hbWUpO1xuICAgIGRlc2NyaXB0b3JbdHlwZV0gPSBjaGFuZ2VkO1xuICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShvYmplY3QsIG5hbWUsIGRlc2NyaXB0b3IpO1xufVxuZnVuY3Rpb24gcHJvdGVjdEZyYW1lUHJvcGVydGllcyh7IGNvbnRleHQsIHdyYXBwZWRXaW5kb3csIGNoYW5nZVdpbmRvd1Byb3BlcnR5LCBzaW5nbGVDYWxsYmFjaywgfSkge1xuICAgIFtcIkhUTUxJRnJhbWVFbGVtZW50XCIsIFwiSFRNTEZyYW1lRWxlbWVudFwiXS5mb3JFYWNoKGZ1bmN0aW9uIChjb25zdHJ1Y3Rvck5hbWUpIHtcbiAgICAgICAgY29uc3QgY29uc3RydWN0b3IgPSBjb250ZXh0W2NvbnN0cnVjdG9yTmFtZV07XG4gICAgICAgIGNvbnN0IHdyYXBwZWRDb25zdHJ1Y3RvciA9IHdyYXBwZWRXaW5kb3dbY29uc3RydWN0b3JOYW1lXTtcbiAgICAgICAgY29uc3QgY29udGVudFdpbmRvd0Rlc2NyaXB0b3IgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKGNvbnN0cnVjdG9yLnByb3RvdHlwZSwgXCJjb250ZW50V2luZG93XCIpO1xuICAgICAgICAvLyBUT0RPOiBDb250aW51ZSBoZXJlISEhIVxuICAgICAgICBjb25zdCBvcmlnaW5hbENvbnRlbnRXaW5kb3dHZXR0ZXIgPSBjb250ZW50V2luZG93RGVzY3JpcHRvci5nZXQ7XG4gICAgICAgIGNvbnN0IGNvbnRlbnRXaW5kb3dUZW1wID0ge1xuICAgICAgICAgICAgZ2V0IGNvbnRlbnRXaW5kb3coKSB7XG4gICAgICAgICAgICAgICAgY29uc3Qgd2luZG93ID0gb3JpZ2luYWxDb250ZW50V2luZG93R2V0dGVyLmNhbGwodGhpcyk7XG4gICAgICAgICAgICAgICAgaWYgKHdpbmRvdykge1xuICAgICAgICAgICAgICAgICAgICBzaW5nbGVDYWxsYmFjayh3aW5kb3cpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICByZXR1cm4gd2luZG93O1xuICAgICAgICAgICAgfSxcbiAgICAgICAgfTtcbiAgICAgICAgY2hhbmdlV2luZG93UHJvcGVydHkod3JhcHBlZENvbnN0cnVjdG9yLnByb3RvdHlwZSwgXCJjb250ZW50V2luZG93XCIsIFwiZ2V0XCIsIE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IoY29udGVudFdpbmRvd1RlbXAsIFwiY29udGVudFdpbmRvd1wiKS5nZXQpO1xuICAgICAgICBjb25zdCBjb250ZW50RG9jdW1lbnREZXNjcmlwdG9yID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihjb25zdHJ1Y3Rvci5wcm90b3R5cGUsIFwiY29udGVudERvY3VtZW50XCIpO1xuICAgICAgICBjb25zdCBvcmlnaW5hbENvbnRlbnREb2N1bWVudEdldHRlciA9IGNvbnRlbnREb2N1bWVudERlc2NyaXB0b3IuZ2V0O1xuICAgICAgICBjb25zdCBjb250ZW50RG9jdW1lbnRUZW1wID0ge1xuICAgICAgICAgICAgZ2V0IGNvbnRlbnREb2N1bWVudCgpIHtcbiAgICAgICAgICAgICAgICBjb25zdCBkb2N1bWVudCA9IG9yaWdpbmFsQ29udGVudERvY3VtZW50R2V0dGVyLmNhbGwodGhpcyk7XG4gICAgICAgICAgICAgICAgaWYgKGRvY3VtZW50KSB7XG4gICAgICAgICAgICAgICAgICAgIHNpbmdsZUNhbGxiYWNrKGRvY3VtZW50LmRlZmF1bHRWaWV3KTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgcmV0dXJuIGRvY3VtZW50O1xuICAgICAgICAgICAgfSxcbiAgICAgICAgfTtcbiAgICAgICAgY2hhbmdlV2luZG93UHJvcGVydHkod3JhcHBlZENvbnN0cnVjdG9yLnByb3RvdHlwZSwgXCJjb250ZW50RG9jdW1lbnRcIiwgXCJnZXRcIiwgT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcihjb250ZW50RG9jdW1lbnRUZW1wLCBcImNvbnRlbnREb2N1bWVudFwiKVxuICAgICAgICAgICAgLmdldCk7XG4gICAgfSk7XG59XG5mdW5jdGlvbiBwcm90ZWN0RE9NTW9kaWZpY2F0aW9ucyh7IHdyYXBwZWRXaW5kb3csIGNoYW5nZVdpbmRvd1Byb3BlcnR5LCBhbGxDYWxsYmFjaywgfSkge1xuICAgIFtcbiAgICAgICAgLy8gdXNlbGVzcyBhcyBsZW5ndGggY291bGQgYmUgb2J0YWluZWQgYmVmb3JlIHRoZSBpZnJhbWUgaXMgY3JlYXRlZCBhbmQgd2luZG93LmZyYW1lcyA9PT0gd2luZG93XG4gICAgICAgIC8vIHtcbiAgICAgICAgLy8gXHRvYmplY3Q6IHdyYXBwZWRXaW5kb3csXG4gICAgICAgIC8vIFx0bWV0aG9kczogW10sXG4gICAgICAgIC8vIFx0Z2V0dGVyczogW1wibGVuZ3RoXCIsIFwiZnJhbWVzXCJdLFxuICAgICAgICAvLyBcdHNldHRlcnM6IFtdXG4gICAgICAgIC8vIH0sXG4gICAgICAgIHtcbiAgICAgICAgICAgIG9iamVjdDogd3JhcHBlZFdpbmRvdy5Ob2RlLnByb3RvdHlwZSxcbiAgICAgICAgICAgIG1ldGhvZHM6IFtcImFwcGVuZENoaWxkXCIsIFwiaW5zZXJ0QmVmb3JlXCIsIFwicmVwbGFjZUNoaWxkXCJdLFxuICAgICAgICAgICAgZ2V0dGVyczogW10sXG4gICAgICAgICAgICBzZXR0ZXJzOiBbXSxcbiAgICAgICAgfSxcbiAgICAgICAge1xuICAgICAgICAgICAgb2JqZWN0OiB3cmFwcGVkV2luZG93LkVsZW1lbnQucHJvdG90eXBlLFxuICAgICAgICAgICAgbWV0aG9kczogW1xuICAgICAgICAgICAgICAgIFwiYXBwZW5kXCIsXG4gICAgICAgICAgICAgICAgXCJwcmVwZW5kXCIsXG4gICAgICAgICAgICAgICAgXCJpbnNlcnRBZGphY2VudEVsZW1lbnRcIixcbiAgICAgICAgICAgICAgICBcImluc2VydEFkamFjZW50SFRNTFwiLFxuICAgICAgICAgICAgICAgIFwiaW5zZXJ0QWRqYWNlbnRUZXh0XCIsXG4gICAgICAgICAgICAgICAgXCJyZXBsYWNlV2l0aFwiLFxuICAgICAgICAgICAgXSxcbiAgICAgICAgICAgIGdldHRlcnM6IFtdLFxuICAgICAgICAgICAgc2V0dGVyczogW1wiaW5uZXJIVE1MXCIsIFwib3V0ZXJIVE1MXCJdLFxuICAgICAgICB9LFxuICAgIF0uZm9yRWFjaChmdW5jdGlvbiAocHJvdGVjdGlvbkRlZmluaXRpb24pIHtcbiAgICAgICAgY29uc3Qgb2JqZWN0ID0gcHJvdGVjdGlvbkRlZmluaXRpb24ub2JqZWN0O1xuICAgICAgICBwcm90ZWN0aW9uRGVmaW5pdGlvbi5tZXRob2RzLmZvckVhY2goZnVuY3Rpb24gKG1ldGhvZCkge1xuICAgICAgICAgICAgY29uc3QgZGVzY3JpcHRvciA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3Iob2JqZWN0LCBtZXRob2QpO1xuICAgICAgICAgICAgY29uc3Qgb3JpZ2luYWwgPSBkZXNjcmlwdG9yLnZhbHVlO1xuICAgICAgICAgICAgY2hhbmdlV2luZG93UHJvcGVydHkob2JqZWN0LCBtZXRob2QsIFwidmFsdWVcIiwgY2xhc3Mge1xuICAgICAgICAgICAgICAgIFttZXRob2RdKCkge1xuICAgICAgICAgICAgICAgICAgICBjb25zdCB2YWx1ZSA9IGFyZ3VtZW50cy5sZW5ndGhcbiAgICAgICAgICAgICAgICAgICAgICAgID8gb3JpZ2luYWwuY2FsbCh0aGlzLCAuLi5hcmd1bWVudHMpXG4gICAgICAgICAgICAgICAgICAgICAgICA6IG9yaWdpbmFsLmNhbGwodGhpcyk7XG4gICAgICAgICAgICAgICAgICAgIGFsbENhbGxiYWNrKCk7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9LnByb3RvdHlwZVttZXRob2RdKTtcbiAgICAgICAgfSk7XG4gICAgICAgIHByb3RlY3Rpb25EZWZpbml0aW9uLmdldHRlcnMuZm9yRWFjaChmdW5jdGlvbiAocHJvcGVydHkpIHtcbiAgICAgICAgICAgIGNvbnN0IHRlbXAgPSB7XG4gICAgICAgICAgICAgICAgZ2V0IFtwcm9wZXJ0eV0oKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IHJldCA9IHRoaXNbcHJvcGVydHldO1xuICAgICAgICAgICAgICAgICAgICBhbGxDYWxsYmFjaygpO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gcmV0O1xuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB9O1xuICAgICAgICAgICAgY2hhbmdlV2luZG93UHJvcGVydHkob2JqZWN0LCBwcm9wZXJ0eSwgXCJnZXRcIiwgT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcih0ZW1wLCBwcm9wZXJ0eSkuZ2V0KTtcbiAgICAgICAgfSk7XG4gICAgICAgIHByb3RlY3Rpb25EZWZpbml0aW9uLnNldHRlcnMuZm9yRWFjaChmdW5jdGlvbiAocHJvcGVydHkpIHtcbiAgICAgICAgICAgIGNvbnN0IGRlc2NyaXB0b3IgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKG9iamVjdCwgcHJvcGVydHkpO1xuICAgICAgICAgICAgY29uc3Qgc2V0dGVyID0gZGVzY3JpcHRvci5zZXQ7XG4gICAgICAgICAgICBjb25zdCB0ZW1wID0ge1xuICAgICAgICAgICAgICAgIHNldChvYmosIF9wcm9wLCB2YWx1ZSkge1xuICAgICAgICAgICAgICAgICAgICBjb25zdCByZXQgPSBzZXR0ZXIuY2FsbChvYmosIHZhbHVlKTtcbiAgICAgICAgICAgICAgICAgICAgYWxsQ2FsbGJhY2soKTtcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHJldDtcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICAgIGNoYW5nZVdpbmRvd1Byb3BlcnR5KG9iamVjdCwgcHJvcGVydHksIFwic2V0XCIsIE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IodGVtcCwgcHJvcGVydHkpLnNldCk7XG4gICAgICAgIH0pO1xuICAgIH0pO1xufVxuZnVuY3Rpb24gZW5hYmxlTXV0YXRpb25PYnNlcnZlcih7IGNvbnRleHQsIGFsbENhbGxiYWNrIH0pIHtcbiAgICBjb25zdCBvYnNlcnZlciA9IG5ldyBNdXRhdGlvbk9ic2VydmVyKGFsbENhbGxiYWNrKTtcbiAgICBsZXQgb2JzZXJ2aW5nID0gZmFsc2U7XG4gICAgZnVuY3Rpb24gb2JzZXJ2ZSgpIHtcbiAgICAgICAgaWYgKCFvYnNlcnZpbmcgJiYgY29udGV4dC5kb2N1bWVudCkge1xuICAgICAgICAgICAgb2JzZXJ2ZXIub2JzZXJ2ZShjb250ZXh0LmRvY3VtZW50LCB7IHN1YnRyZWU6IHRydWUsIGNoaWxkTGlzdDogdHJ1ZSB9KTtcbiAgICAgICAgICAgIG9ic2VydmluZyA9IHRydWU7XG4gICAgICAgIH1cbiAgICB9XG4gICAgb2JzZXJ2ZSgpO1xuICAgIGNvbnRleHQuZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcihcIkRPTUNvbnRlbnRMb2FkZWRcIiwgZnVuY3Rpb24gKCkge1xuICAgICAgICBpZiAob2JzZXJ2aW5nKSB7XG4gICAgICAgICAgICBvYnNlcnZlci5kaXNjb25uZWN0KCk7XG4gICAgICAgICAgICBvYnNlcnZpbmcgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiBvYnNlcnZlO1xufVxuZnVuY3Rpb24gcHJvdGVjdERvY3VtZW50V3JpdGUoeyBjb250ZXh0LCB3cmFwcGVkV2luZG93LCBjaGFuZ2VXaW5kb3dQcm9wZXJ0eSwgb2JzZXJ2ZSwgYWxsQ2FsbGJhY2ssIH0pIHtcbiAgICBjb25zdCBkb2N1bWVudFdyaXRlRGVzY3JpcHRvck9uSFRNTERvY3VtZW50ID0gT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcih3cmFwcGVkV2luZG93LkhUTUxEb2N1bWVudC5wcm90b3R5cGUsIFwid3JpdGVcIik7XG4gICAgY29uc3QgZG9jdW1lbnRXcml0ZURlc2NyaXB0b3IgPSBkb2N1bWVudFdyaXRlRGVzY3JpcHRvck9uSFRNTERvY3VtZW50IHx8XG4gICAgICAgIE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3Iod3JhcHBlZFdpbmRvdy5Eb2N1bWVudC5wcm90b3R5cGUsIFwid3JpdGVcIik7XG4gICAgY29uc3QgZG9jdW1lbnRXcml0ZSA9IGRvY3VtZW50V3JpdGVEZXNjcmlwdG9yLnZhbHVlO1xuICAgIGNoYW5nZVdpbmRvd1Byb3BlcnR5KGRvY3VtZW50V3JpdGVEZXNjcmlwdG9yT25IVE1MRG9jdW1lbnRcbiAgICAgICAgPyB3cmFwcGVkV2luZG93LkhUTUxEb2N1bWVudC5wcm90b3R5cGVcbiAgICAgICAgOiB3cmFwcGVkV2luZG93LkRvY3VtZW50LnByb3RvdHlwZSwgXCJ3cml0ZVwiLCBcInZhbHVlXCIsIGZ1bmN0aW9uIHdyaXRlKF9tYXJrdXApIHtcbiAgICAgICAgZm9yIChsZXQgaSA9IDAsIGwgPSBhcmd1bWVudHMubGVuZ3RoOyBpIDwgbDsgaSArPSAxKSB7XG4gICAgICAgICAgICBjb25zdCBzdHIgPSBcIlwiICsgYXJndW1lbnRzW2ldO1xuICAgICAgICAgICAgLy8gd2VpcmQgcHJvYmxlbSB3aXRoIHdhdGVyZm94IGFuZCBnb29nbGUgZG9jc1xuICAgICAgICAgICAgY29uc3QgcGFydHMgPSBzdHIubWF0Y2goL15cXHMqPCFkb2N0eXBlL2kpICYmICFzdHIubWF0Y2goL2ZyYW1lL2kpXG4gICAgICAgICAgICAgICAgPyBbc3RyXVxuICAgICAgICAgICAgICAgIDogc3RyLnNwbGl0KC8oPz08KS8pO1xuICAgICAgICAgICAgY29uc3QgbGVuZ3RoID0gcGFydHMubGVuZ3RoO1xuICAgICAgICAgICAgY29uc3Qgc2NyaXB0cyA9IGNvbnRleHQuZG9jdW1lbnQuZ2V0RWxlbWVudHNCeVRhZ05hbWUoXCJzY3JpcHRcIik7XG4gICAgICAgICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGxlbmd0aDsgaSArPSAxKSB7XG4gICAgICAgICAgICAgICAgZG9jdW1lbnRXcml0ZS5jYWxsKHRoaXMsIHBhcnRzW2ldKTtcbiAgICAgICAgICAgICAgICBhbGxDYWxsYmFjaygpO1xuICAgICAgICAgICAgICAgIGlmIChzY3JpcHRzLmxlbmd0aCAmJiBzY3JpcHRzW3NjcmlwdHMubGVuZ3RoIC0gMV0uc3JjKSB7XG4gICAgICAgICAgICAgICAgICAgIG9ic2VydmUoKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICB9KTtcbiAgICBjb25zdCBkb2N1bWVudFdyaXRlbG5EZXNjcmlwdG9yT25IVE1MRG9jdW1lbnQgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKHdyYXBwZWRXaW5kb3cuSFRNTERvY3VtZW50LnByb3RvdHlwZSwgXCJ3cml0ZWxuXCIpO1xuICAgIGNvbnN0IGRvY3VtZW50V3JpdGVsbkRlc2NyaXB0b3IgPSBkb2N1bWVudFdyaXRlbG5EZXNjcmlwdG9yT25IVE1MRG9jdW1lbnQgfHxcbiAgICAgICAgT2JqZWN0LmdldE93blByb3BlcnR5RGVzY3JpcHRvcih3cmFwcGVkV2luZG93LkRvY3VtZW50LnByb3RvdHlwZSwgXCJ3cml0ZWxuXCIpO1xuICAgIGNvbnN0IGRvY3VtZW50V3JpdGVsbiA9IGRvY3VtZW50V3JpdGVsbkRlc2NyaXB0b3IudmFsdWU7XG4gICAgY2hhbmdlV2luZG93UHJvcGVydHkoZG9jdW1lbnRXcml0ZWxuRGVzY3JpcHRvck9uSFRNTERvY3VtZW50XG4gICAgICAgID8gd3JhcHBlZFdpbmRvdy5IVE1MRG9jdW1lbnQucHJvdG90eXBlXG4gICAgICAgIDogd3JhcHBlZFdpbmRvdy5Eb2N1bWVudC5wcm90b3R5cGUsIFwid3JpdGVsblwiLCBcInZhbHVlXCIsIGZ1bmN0aW9uIHdyaXRlbG4oX21hcmt1cCkge1xuICAgICAgICBmb3IgKGxldCBpID0gMCwgbCA9IGFyZ3VtZW50cy5sZW5ndGg7IGkgPCBsOyBpICs9IDEpIHtcbiAgICAgICAgICAgIGNvbnN0IHN0ciA9IFwiXCIgKyBhcmd1bWVudHNbaV07XG4gICAgICAgICAgICBjb25zdCBwYXJ0cyA9IHN0ci5zcGxpdCgvKD89PCkvKTtcbiAgICAgICAgICAgIGNvbnN0IGxlbmd0aCA9IHBhcnRzLmxlbmd0aDtcbiAgICAgICAgICAgIGNvbnN0IHNjcmlwdHMgPSBjb250ZXh0LmRvY3VtZW50LmdldEVsZW1lbnRzQnlUYWdOYW1lKFwic2NyaXB0XCIpO1xuICAgICAgICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBsZW5ndGg7IGkgKz0gMSkge1xuICAgICAgICAgICAgICAgIGRvY3VtZW50V3JpdGUuY2FsbCh0aGlzLCBwYXJ0c1tpXSk7XG4gICAgICAgICAgICAgICAgYWxsQ2FsbGJhY2soKTtcbiAgICAgICAgICAgICAgICBpZiAoc2NyaXB0cy5sZW5ndGggJiYgc2NyaXB0c1tzY3JpcHRzLmxlbmd0aCAtIDFdLnNyYykge1xuICAgICAgICAgICAgICAgICAgICBvYnNlcnZlKCk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGRvY3VtZW50V3JpdGVsbi5jYWxsKHRoaXMsIFwiXCIpO1xuICAgIH0pO1xufVxuZnVuY3Rpb24gcHJvdGVjdFdpbmRvd09wZW4oeyBjb250ZXh0LCB3cmFwcGVkV2luZG93LCBjaGFuZ2VXaW5kb3dQcm9wZXJ0eSwgc2luZ2xlQ2FsbGJhY2ssIH0pIHtcbiAgICBjb25zdCB3aW5kb3dPcGVuRGVzY3JpcHRvciA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3Iod3JhcHBlZFdpbmRvdywgXCJvcGVuXCIpO1xuICAgIGNvbnN0IHdpbmRvd09wZW4gPSB3aW5kb3dPcGVuRGVzY3JpcHRvci52YWx1ZTtcbiAgICBjb25zdCBnZXREb2N1bWVudCA9IE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IoY29udGV4dCwgXCJkb2N1bWVudFwiKS5nZXQ7XG4gICAgY2hhbmdlV2luZG93UHJvcGVydHkod3JhcHBlZFdpbmRvdywgXCJvcGVuXCIsIFwidmFsdWVcIiwgZnVuY3Rpb24gb3BlbigpIHtcbiAgICAgICAgY29uc3QgbmV3V2luZG93ID0gYXJndW1lbnRzLmxlbmd0aFxuICAgICAgICAgICAgPyB3aW5kb3dPcGVuLmNhbGwodGhpcywgLi4uYXJndW1lbnRzKVxuICAgICAgICAgICAgOiB3aW5kb3dPcGVuLmNhbGwodGhpcyk7XG4gICAgICAgIGlmIChuZXdXaW5kb3cpIHtcbiAgICAgICAgICAgIC8vIGlmIHdlIHVzZSB3aW5kb3dPcGVuIGZyb20gdGhlIG5vcm1hbCB3aW5kb3cgd2Ugc2VlIHNvbWUgU09QIGVycm9yc1xuICAgICAgICAgICAgLy8gQlVUIHdlIG5lZWQgdGhlIHVud3JhcHBlZCB3aW5kb3cuLi5cbiAgICAgICAgICAgIHNpbmdsZUNhbGxiYWNrKGdldERvY3VtZW50LmNhbGwobmV3V2luZG93KS5kZWZhdWx0Vmlldyk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIG5ld1dpbmRvdztcbiAgICB9KTtcbn1cbi8vIyBzb3VyY2VNYXBwaW5nVVJMPWRhdGE6YXBwbGljYXRpb24vanNvbjtiYXNlNjQsZXlKMlpYSnphVzl1SWpvekxDSm1hV3hsSWpvaWMzUmxZV3gwYUM1cWN5SXNJbk52ZFhKalpWSnZiM1FpT2lJaUxDSnpiM1Z5WTJWeklqcGJJaTR1THk0dUx5NHVMM055WXk5emRHVmhiSFJvTDNOMFpXRnNkR2d1ZEhNaVhTd2libUZ0WlhNaU9sdGRMQ0p0WVhCd2FXNW5jeUk2SWtGQlFVRXNXVUZCV1N4RFFVRkRPenRCUVVOaU96czdPRVJCUnpoRU8wRkJSVGxFTERaRFFVZHpRanRCUVVWMFFpeG5RMEZCWjBNN1FVRkRhRU1zVFVGQlRTeHJRa0ZCYTBJc1IwRkJSeXhKUVVGSkxFOUJRVThzUlVGQlJTeERRVUZETzBGQlEzcERMRTFCUVUwc1QwRkJUeXhIUVVGSExFbEJRVWtzUjBGQlJ5eEZRVUZGTEVOQlFVTTdRVUZETVVJc1RVRkJUU3huUWtGQlowSXNSMEZCUnl4SlFVRkpMRTlCUVU4c1JVRkJSU3hEUVVGRE8wRkJSM1pETEdsRFFVRnBRenRCUVVOcVF5eERRVUZETzBsQlEwTXNkME5CUVhkRE8wbEJRM2hETEVsQlFVa3NRMEZCUXp0UlFVTklMR1ZCUVdVc1EwRkJReXhOUVVGM1FpeERRVUZETEVOQlFVTTdTVUZETlVNc1EwRkJRenRKUVVGRExFOUJRVThzUzBGQlN5eEZRVUZGTEVOQlFVTTdVVUZEWml4UFFVRlBMRU5CUVVNc1IwRkJSeXhEUVVGRExHdEVRVUZyUkN4SFFVRkhMRXRCUVVzc1EwRkJReXhEUVVGRE8xRkJRM2hGTEU5QlFVOHNRMEZCUXl4SFFVRkhMRU5CUVVNc1MwRkJTeXhEUVVGRExFdEJRVXNzUTBGQlF5eERRVUZETzBsQlF6TkNMRU5CUVVNN1NVRkRSQ3gzUTBGQmQwTTdRVUZETVVNc1EwRkJReXhEUVVGRExFVkJRVVVzUTBGQlF6dEJRVVZNTEZOQlFWTXNaVUZCWlN4RFFVRkRMRTlCUVhWQ08wbEJRemxETEVsQlFVa3NWVUZCVlN4RFFVRkRPMGxCUTJZc1NVRkJTU3hEUVVGRE8xRkJRMGdzVlVGQlZTeEhRVUZITEZWQlFWVXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJRenRKUVVOdVF5eERRVUZETzBsQlFVTXNUMEZCVHl4TFFVRkxMRVZCUVVVc1EwRkJRenRSUVVObUxHZEVRVUZuUkR0UlFVTm9SQ3huUkVGQlowUTdVVUZEYUVRc1owVkJRV2RGTzFGQlEyaEZMRTlCUVU4c1MwRkJTeXhEUVVGRE8wbEJRMllzUTBGQlF6dEpRVU5FTEUxQlFVMHNZVUZCWVN4SFFVRkhMRlZCUVZVc1EwRkJRenRKUVVWcVF5eEpRVUZKTEd0Q1FVRnJRaXhEUVVGRExFZEJRVWNzUTBGQlF5eGhRVUZoTEVOQlFVTXNSVUZCUlN4RFFVRkRPMUZCUXpGRExHbEVRVUZwUkR0UlFVTnFSQ3hQUVVGUExFdEJRVXNzUTBGQlF6dEpRVU5tTEVOQlFVTTdTVUZEUkN3clEwRkJLME03U1VGREwwTXNTVUZCUVN3MFFrRkJWU3hGUVVGRExFOUJRVThzUTBGQlF5eERRVUZETzBsQlEzQkNMR3RDUVVGclFpeERRVUZETEVkQlFVY3NRMEZCUXl4aFFVRmhMRVZCUVVVc1NVRkJTU3hEUVVGRExFTkJRVU03U1VGRk5VTXNkVVZCUVhWRk8wbEJRM1pGTEZOQlFWTXNhMEpCUVd0Q08xRkJRM3BDTEUxQlFVMHNZVUZCWVN4SFFVRkhMRTlCUVU4c1EwRkJReXhOUVVGTkxFTkJRVU03VVVGRGNrTXNTMEZCU3l4SlFVRkpMRU5CUVVNc1IwRkJSeXhoUVVGaExFVkJRVVVzUTBGQlF5eEZRVUZGTEVkQlFVa3NRMEZCUXp0WlFVTnNReXhKUVVGSkxFTkJRVU1zYTBKQlFXdENMRU5CUVVNc1IwRkJSeXhEUVVGRExHRkJRV0VzUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXl4RlFVRkZMRU5CUVVNN1owSkJRemxETEdWQlFXVXNRMEZCUXl4UFFVRlBMRU5CUVVNc1EwRkJReXhEUVVGdFFpeERRVUZETEVOQlFVTTdXVUZEYUVRc1EwRkJRenRSUVVOSUxFTkJRVU03U1VGRFNDeERRVUZETzBsQlEwUXNaMEpCUVdkQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEdGQlFXRXNSVUZCUlN4bFFVRmxMRVZCUVVVc2EwSkJRV3RDTEVOQlFVTXNRMEZCUXp0SlFVTTVSU3hQUVVGUExFbEJRVWtzUTBGQlF6dEJRVU5rTEVOQlFVTTdRVUZGUkN4VFFVRlRMR2RDUVVGblFpeERRVUZETEU5QlFVOHNSVUZCUlN4aFFVRmhMRVZCUVVVc1kwRkJZeXhGUVVGRkxGZEJRVmM3U1VGRE0wVXNUVUZCVFN4dlFrRkJiMElzUjBGQlJ5eHZRa0ZCYjBJc1EwRkJReXhQUVVGUExFTkJRVU1zUTBGQlF6dEpRVU16UkN4SlFVRkpMRU5CUVVNc2IwSkJRVzlDTEVWQlFVVXNRMEZCUXp0UlFVTXhRaXhQUVVGUE8wbEJRMVFzUTBGQlF6dEpRVVZFTEUxQlFVMHNSMEZCUnl4SFFVRkhPMUZCUTFZc1QwRkJUenRSUVVOUUxHRkJRV0U3VVVGRFlpeHZRa0ZCYjBJN1VVRkRjRUlzWTBGQll6dFJRVU5rTEZkQlFWYzdVVUZEV0N4UFFVRlBMRVZCUVVVc1NVRkJTVHRMUVVOa0xFTkJRVU03U1VGRlJpeHpRa0ZCYzBJc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF6dEpRVVUxUWl4MVFrRkJkVUlzUTBGQlF5eEhRVUZITEVOQlFVTXNRMEZCUXp0SlFVVTNRaXhyUlVGQmEwVTdTVUZEYkVVc1IwRkJSeXhEUVVGRExFOUJRVThzUjBGQlJ5eHpRa0ZCYzBJc1EwRkJReXhIUVVGSExFTkJRVU1zUTBGQlF6dEpRVVV4UXl3MFJVRkJORVU3U1VGRE5VVXNiMEpCUVc5Q0xFTkJRVU1zUjBGQlJ5eERRVUZETEVOQlFVTTdTVUZGTVVJc2FVSkJRV2xDTEVOQlFVTXNSMEZCUnl4RFFVRkRMRU5CUVVNN1FVRkRla0lzUTBGQlF6dEJRVVZFTEZOQlFWTXNWVUZCVlN4RFFVTnFRaXhQUVVFNFJEdEpRVVU1UkN4UFFVRlBMRTlCUVU4c1NVRkJTU3hEUVVGRExFOUJRVThzUTBGQlF5eGxRVUZsTEVsQlFVa3NUMEZCVHl4RFFVRkRMRU5CUVVNN1FVRkRla1FzUTBGQlF6dEJRVVZFTEZOQlFWTXNiMEpCUVc5Q0xFTkJRVU1zVFVGQlRUdEpRVU5zUXl4TlFVRk5MRzlDUVVGdlFpeEhRVUZITEZWQlFWVXNUVUZCVFN4RlFVRkZMRWxCUVVrc1JVRkJSU3hKUVVGSkxFVkJRVVVzVDBGQlR6dFJRVU5vUlN4TlFVRk5MRlZCUVZVc1IwRkJSeXhOUVVGTkxFTkJRVU1zZDBKQlFYZENMRU5CUVVNc1RVRkJUU3hGUVVGRkxFbEJRVWtzUTBGQlF5eERRVUZETzFGQlEycEZMRTFCUVUwc1VVRkJVU3hIUVVGSExGVkJRVlVzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0UlFVTnNReXhKUVVGSkxFOUJRVThzVDBGQlR5eExRVUZMTEZWQlFWVXNSVUZCUlN4RFFVRkRPMWxCUTJ4RExFOUJRVThzUjBGQlJ5eHRRa0ZCYlVJc1EwRkJReXhOUVVGTkxFVkJRVVVzVVVGQlVTeEZRVUZGTEU5QlFVOHNRMEZCUXl4RFFVRkRPMUZCUXpORUxFTkJRVU03VVVGRFJDeHJRa0ZCYTBJc1EwRkJReXhOUVVGTkxFVkJRVVVzUlVGQlJTeE5RVUZOTEVWQlFVVXNTVUZCU1N4RlFVRkZMRWxCUVVrc1JVRkJSU3hQUVVGUExFVkJRVVVzUTBGQlF5eERRVUZETzBsQlF6bEVMRU5CUVVNc1EwRkJRenRKUVVOR0xFOUJRVThzYjBKQlFXOUNMRU5CUVVNN1FVRkRPVUlzUTBGQlF6dEJRVVZFTEZOQlFWTXNiVUpCUVcxQ0xFTkJRVU1zVDBGQlR5eEZRVUZGTEZGQlFWRXNSVUZCUlN4WFFVRlhPMGxCUTNwRUxFbEJRVWtzUTBGQlF5eG5Ra0ZCWjBJc1EwRkJReXhIUVVGSExFTkJRVU1zVDBGQlR5eERRVUZETEVWQlFVVXNRMEZCUXp0UlFVTnVReXhuUWtGQlowSXNRMEZCUXl4SFFVRkhMRU5CUVVNc1QwRkJUeXhGUVVGRkxFbEJRVWtzUTBGQlF5eERRVUZETzFGQlEzQkRMRTFCUVUwc2FVSkJRV2xDTEVkQlFVY3NWVUZCVlN4RFFVRkRMRTlCUVU4c1EwRkJReXhEUVVGRExGRkJRVkVzUTBGQlF5eFRRVUZUTEVOQlFVTTdVVUZEYWtVc1RVRkJUU3hSUVVGUkxFZEJRVWNzYVVKQlFXbENMRU5CUVVNc1VVRkJVU3hEUVVGRE8xRkJRelZETEd0Q1FVRnJRaXhEUVVGRExFOUJRVThzUlVGQlJUdFpRVU14UWl4TlFVRk5MRVZCUVVVc2FVSkJRV2xDTzFsQlEzcENMRWxCUVVrc1JVRkJSU3hWUVVGVk8xbEJRMmhDTEVsQlFVa3NSVUZCUlN4UFFVRlBPMWxCUTJJc1QwRkJUeXhGUVVGRkxHMUNRVUZ0UWl4RFFVRkRMRTlCUVU4c1JVRkJSU3hSUVVGUkxFVkJRVVU3WjBKQlF6bERMRTlCUVU4c1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF5eEpRVUZKTEVOQlFVTXNTVUZCU1N4UlFVRlJMRU5CUVVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUTBGQlF5eERRVUZETzFsQlEyeEVMRU5CUVVNc1EwRkJRenRUUVVOSUxFTkJRVU1zUTBGQlF6dEpRVU5NTEVOQlFVTTdTVUZEUkN4TlFVRk5MRTlCUVU4c1IwRkJSeXhWUVVGVkxFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTXNUVUZCVFN4RFFVRkRMRTFCUVUwc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF6dEpRVU40UkN4UFFVRlBMRU5CUVVNc1MwRkJTeXhIUVVGSExFbEJRVUVzYVVOQlFXOUNMRVZCUTJ4RExGVkJRVlVzVFVGQlRTeEZRVUZGTEZGQlFWRXNSVUZCUlN4SlFVRkpPMUZCUXpsQ0xFbEJRVWtzUTBGQlF6dFpRVU5JTEU5QlFVOHNTVUZCU1N4RFFVRkRMRTFCUVUwN1owSkJRMmhDTEVOQlFVTXNRMEZCUXl4WFFVRlhMRU5CUVVNc1NVRkJTU3hEUVVGRExGRkJRVkVzUlVGQlJTeEhRVUZITEVsQlFVa3NRMEZCUXp0blFrRkRja01zUTBGQlF5eERRVUZETEZkQlFWY3NRMEZCUXl4SlFVRkpMRU5CUVVNc1VVRkJVU3hEUVVGRExFTkJRVU03VVVGRGFrTXNRMEZCUXp0UlFVRkRMRTlCUVU4c1MwRkJTeXhGUVVGRkxFTkJRVU03V1VGRFppeEpRVUZKTEVOQlFVTTdaMEpCUTBnc1QwRkJUeXhSUVVGUkxFTkJRVU1zUzBGQlN5eERRVUZETEZGQlFWRXNSVUZCUlN4SlFVRkpMRU5CUVVNc1EwRkJRenRaUVVONFF5eERRVUZETzFsQlFVTXNUMEZCVHl4TFFVRkxMRVZCUVVVc1EwRkJRenRuUWtGRFppeFBRVUZQTEUxQlFVMHNRMEZCUXl4TFFVRkxMRU5CUVVNc1VVRkJVU3hGUVVGRkxFbEJRVWtzUTBGQlF5eERRVUZETzFsQlEzUkRMRU5CUVVNN1VVRkRTQ3hEUVVGRE8wbEJRMGdzUTBGQlF5eEZRVU5FTEU5QlFVOHNSVUZEVUN4RlFVRkZMRU5CUTBnc1EwRkJRenRKUVVOR0xFMUJRVTBzUzBGQlN5eEhRVUZITEVsQlFVa3NUMEZCVHl4RFFVRkRMRXRCUVVzc1EwRkJReXhSUVVGUkxFVkJRVVVzVDBGQlR5eERRVUZETEVOQlFVTTdTVUZEYmtRc1QwRkJUeXhEUVVGRExFZEJRVWNzUTBGQlF5eExRVUZMTEVWQlFVVXNVVUZCVVN4RFFVRkRMRkZCUVZFc1JVRkJSU3hEUVVGRExFTkJRVU03U1VGRGVFTXNUMEZCVHl4VlFVRlZMRU5CUVVNc1MwRkJTeXhEUVVGRExFTkJRVU03UVVGRE0wSXNRMEZCUXp0QlFVVkVMRk5CUVZNc2EwSkJRV3RDTEVOQlFVTXNVVUZCVVN4RlFVRkZMRVZCUVVVc1RVRkJUU3hGUVVGRkxFbEJRVWtzUlVGQlJTeEpRVUZKTEVWQlFVVXNUMEZCVHl4RlFVRkZPMGxCUTI1RkxIbERRVUY1UXp0SlFVTjZReXhOUVVGTkxGVkJRVlVzUjBGQlJ5eE5RVUZOTEVOQlFVTXNkMEpCUVhkQ0xFTkJRVU1zVFVGQlRTeEZRVUZGTEVsQlFVa3NRMEZCUXl4RFFVRkRPMGxCUTJwRkxGVkJRVlVzUTBGQlF5eEpRVUZKTEVOQlFVTXNSMEZCUnl4UFFVRlBMRU5CUVVNN1NVRkRNMElzVFVGQlRTeERRVUZETEdOQlFXTXNRMEZCUXl4TlFVRk5MRVZCUVVVc1NVRkJTU3hGUVVGRkxGVkJRVlVzUTBGQlF5eERRVUZETzBGQlEyeEVMRU5CUVVNN1FVRkZSQ3hUUVVGVExITkNRVUZ6UWl4RFFVRkRMRVZCUXpsQ0xFOUJRVThzUlVGRFVDeGhRVUZoTEVWQlEySXNiMEpCUVc5Q0xFVkJRM0JDTEdOQlFXTXNSMEZEWmp0SlFVTkRMRU5CUVVNc2JVSkJRVzFDTEVWQlFVVXNhMEpCUVd0Q0xFTkJRVU1zUTBGQlF5eFBRVUZQTEVOQlFVTXNWVUZCVlN4bFFVRmxPMUZCUTNwRkxFMUJRVTBzVjBGQlZ5eEhRVUZITEU5QlFVOHNRMEZCUXl4bFFVRmxMRU5CUVVNc1EwRkJRenRSUVVNM1F5eE5RVUZOTEd0Q1FVRnJRaXhIUVVGSExHRkJRV0VzUTBGQlF5eGxRVUZsTEVOQlFVTXNRMEZCUXp0UlFVVXhSQ3hOUVVGTkxIVkNRVUYxUWl4SFFVRkhMRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZETjBRc1YwRkJWeXhEUVVGRExGTkJRVk1zUlVGRGNrSXNaVUZCWlN4RFFVTm9RaXhEUVVGRE8xRkJRMFlzTUVKQlFUQkNPMUZCUXpGQ0xFMUJRVTBzTWtKQlFUSkNMRWRCUVVjc2RVSkJRWFZDTEVOQlFVTXNSMEZCUnl4RFFVRkRPMUZCUTJoRkxFMUJRVTBzYVVKQlFXbENMRWRCUVVjN1dVRkRlRUlzU1VGQlNTeGhRVUZoTzJkQ1FVTm1MRTFCUVUwc1RVRkJUU3hIUVVGSExESkNRVUV5UWl4RFFVRkRMRWxCUVVrc1EwRkJReXhKUVVGSkxFTkJRVU1zUTBGQlF6dG5Ra0ZEZEVRc1NVRkJTU3hOUVVGTkxFVkJRVVVzUTBGQlF6dHZRa0ZEV0N4alFVRmpMRU5CUVVNc1RVRkJUU3hEUVVGRExFTkJRVU03WjBKQlEzcENMRU5CUVVNN1owSkJRMFFzVDBGQlR5eE5RVUZOTEVOQlFVTTdXVUZEYUVJc1EwRkJRenRUUVVOR0xFTkJRVU03VVVGRFJpeHZRa0ZCYjBJc1EwRkRiRUlzYTBKQlFXdENMRU5CUVVNc1UwRkJVeXhGUVVNMVFpeGxRVUZsTEVWQlEyWXNTMEZCU3l4RlFVTk1MRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4cFFrRkJhVUlzUlVGQlJTeGxRVUZsTEVOQlFVTXNRMEZCUXl4SFFVRkhMRU5CUTNoRkxFTkJRVU03VVVGRlJpeE5RVUZOTEhsQ1FVRjVRaXhIUVVGSExFMUJRVTBzUTBGQlF5eDNRa0ZCZDBJc1EwRkRMMFFzVjBGQlZ5eERRVUZETEZOQlFWTXNSVUZEY2tJc2FVSkJRV2xDTEVOQlEyeENMRU5CUVVNN1VVRkRSaXhOUVVGTkxEWkNRVUUyUWl4SFFVRkhMSGxDUVVGNVFpeERRVUZETEVkQlFVY3NRMEZCUXp0UlFVTndSU3hOUVVGTkxHMUNRVUZ0UWl4SFFVRkhPMWxCUXpGQ0xFbEJRVWtzWlVGQlpUdG5Ra0ZEYWtJc1RVRkJUU3hSUVVGUkxFZEJRVWNzTmtKQlFUWkNMRU5CUVVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUTBGQlF5eERRVUZETzJkQ1FVTXhSQ3hKUVVGSkxGRkJRVkVzUlVGQlJTeERRVUZETzI5Q1FVTmlMR05CUVdNc1EwRkJReXhSUVVGUkxFTkJRVU1zVjBGQlZ5eERRVUZETEVOQlFVTTdaMEpCUTNaRExFTkJRVU03WjBKQlEwUXNUMEZCVHl4UlFVRlJMRU5CUVVNN1dVRkRiRUlzUTBGQlF6dFRRVU5HTEVOQlFVTTdVVUZEUml4dlFrRkJiMElzUTBGRGJFSXNhMEpCUVd0Q0xFTkJRVU1zVTBGQlV5eEZRVU0xUWl4cFFrRkJhVUlzUlVGRGFrSXNTMEZCU3l4RlFVTk1MRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4dFFrRkJiVUlzUlVGQlJTeHBRa0ZCYVVJc1EwRkJRenRoUVVOd1JTeEhRVUZITEVOQlExQXNRMEZCUXp0SlFVTktMRU5CUVVNc1EwRkJReXhEUVVGRE8wRkJRMHdzUTBGQlF6dEJRVVZFTEZOQlFWTXNkVUpCUVhWQ0xFTkJRVU1zUlVGREwwSXNZVUZCWVN4RlFVTmlMRzlDUVVGdlFpeEZRVU53UWl4WFFVRlhMRWRCUTFvN1NVRkRRenRSUVVORkxHZEhRVUZuUnp0UlFVTm9SeXhKUVVGSk8xRkJRMG9zTUVKQlFUQkNPMUZCUXpGQ0xHZENRVUZuUWp0UlFVTm9RaXhyUTBGQmEwTTdVVUZEYkVNc1pVRkJaVHRSUVVObUxFdEJRVXM3VVVGRFREdFpRVU5GTEUxQlFVMHNSVUZCUlN4aFFVRmhMRU5CUVVNc1NVRkJTU3hEUVVGRExGTkJRVk03V1VGRGNFTXNUMEZCVHl4RlFVRkZMRU5CUVVNc1lVRkJZU3hGUVVGRkxHTkJRV01zUlVGQlJTeGpRVUZqTEVOQlFVTTdXVUZEZUVRc1QwRkJUeXhGUVVGRkxFVkJRVVU3V1VGRFdDeFBRVUZQTEVWQlFVVXNSVUZCUlR0VFFVTmFPMUZCUTBRN1dVRkRSU3hOUVVGTkxFVkJRVVVzWVVGQllTeERRVUZETEU5QlFVOHNRMEZCUXl4VFFVRlRPMWxCUTNaRExFOUJRVThzUlVGQlJUdG5Ra0ZEVUN4UlFVRlJPMmRDUVVOU0xGTkJRVk03WjBKQlExUXNkVUpCUVhWQ08yZENRVU4yUWl4dlFrRkJiMEk3WjBKQlEzQkNMRzlDUVVGdlFqdG5Ra0ZEY0VJc1lVRkJZVHRoUVVOa08xbEJRMFFzVDBGQlR5eEZRVUZGTEVWQlFVVTdXVUZEV0N4UFFVRlBMRVZCUVVVc1EwRkJReXhYUVVGWExFVkJRVVVzVjBGQlZ5eERRVUZETzFOQlEzQkRPMHRCUTBZc1EwRkJReXhQUVVGUExFTkJRVU1zVlVGQlZTeHZRa0ZCYjBJN1VVRkRkRU1zVFVGQlRTeE5RVUZOTEVkQlFVY3NiMEpCUVc5Q0xFTkJRVU1zVFVGQlRTeERRVUZETzFGQlF6TkRMRzlDUVVGdlFpeERRVUZETEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNc1ZVRkJWU3hOUVVGTk8xbEJRMjVFTEUxQlFVMHNWVUZCVlN4SFFVRkhMRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4TlFVRk5MRVZCUVVVc1RVRkJUU3hEUVVGRExFTkJRVU03V1VGRGJrVXNUVUZCVFN4UlFVRlJMRWRCUVVjc1ZVRkJWU3hEUVVGRExFdEJRVXNzUTBGQlF6dFpRVU5zUXl4dlFrRkJiMElzUTBGRGJFSXNUVUZCVFN4RlFVTk9MRTFCUVUwc1JVRkRUaXhQUVVGUExFVkJRMUE3WjBKQlEwVXNRMEZCUXl4TlFVRk5MRU5CUVVNN2IwSkJRMDRzVFVGQlRTeExRVUZMTEVkQlFVY3NVMEZCVXl4RFFVRkRMRTFCUVUwN2QwSkJRelZDTEVOQlFVTXNRMEZCUXl4UlFVRlJMRU5CUVVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUlVGQlJTeEhRVUZITEZOQlFWTXNRMEZCUXp0M1FrRkRia01zUTBGQlF5eERRVUZETEZGQlFWRXNRMEZCUXl4SlFVRkpMRU5CUVVNc1NVRkJTU3hEUVVGRExFTkJRVU03YjBKQlEzaENMRmRCUVZjc1JVRkJSU3hEUVVGRE8yOUNRVU5rTEU5QlFVOHNTMEZCU3l4RFFVRkRPMmRDUVVObUxFTkJRVU03WVVGRFJpeERRVUZETEZOQlFWTXNRMEZCUXl4TlFVRk5MRU5CUVVNc1EwRkRjRUlzUTBGQlF6dFJRVU5LTEVOQlFVTXNRMEZCUXl4RFFVRkRPMUZCUTBnc2IwSkJRVzlDTEVOQlFVTXNUMEZCVHl4RFFVRkRMRTlCUVU4c1EwRkJReXhWUVVGVkxGRkJRVkU3V1VGRGNrUXNUVUZCVFN4SlFVRkpMRWRCUVVjN1owSkJRMWdzU1VGQlNTeERRVUZETEZGQlFWRXNRMEZCUXp0dlFrRkRXaXhOUVVGTkxFZEJRVWNzUjBGQlJ5eEpRVUZKTEVOQlFVTXNVVUZCVVN4RFFVRkRMRU5CUVVNN2IwSkJRek5DTEZkQlFWY3NSVUZCUlN4RFFVRkRPMjlDUVVOa0xFOUJRVThzUjBGQlJ5eERRVUZETzJkQ1FVTmlMRU5CUVVNN1lVRkRSaXhEUVVGRE8xbEJRMFlzYjBKQlFXOUNMRU5CUTJ4Q0xFMUJRVTBzUlVGRFRpeFJRVUZSTEVWQlExSXNTMEZCU3l4RlFVTk1MRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4SlFVRkpMRVZCUVVVc1VVRkJVU3hEUVVGRExFTkJRVU1zUjBGQlJ5eERRVU53UkN4RFFVRkRPMUZCUTBvc1EwRkJReXhEUVVGRExFTkJRVU03VVVGRFNDeHZRa0ZCYjBJc1EwRkJReXhQUVVGUExFTkJRVU1zVDBGQlR5eERRVUZETEZWQlFWVXNVVUZCVVR0WlFVTnlSQ3hOUVVGTkxGVkJRVlVzUjBGQlJ5eE5RVUZOTEVOQlFVTXNkMEpCUVhkQ0xFTkJRVU1zVFVGQlRTeEZRVUZGTEZGQlFWRXNRMEZCUXl4RFFVRkRPMWxCUTNKRkxFMUJRVTBzVFVGQlRTeEhRVUZITEZWQlFWVXNRMEZCUXl4SFFVRkhMRU5CUVVNN1dVRkRPVUlzVFVGQlRTeEpRVUZKTEVkQlFVYzdaMEpCUTFnc1IwRkJSeXhEUVVGRExFZEJRVWNzUlVGQlJTeExRVUZMTEVWQlFVVXNTMEZCU3p0dlFrRkRia0lzVFVGQlRTeEhRVUZITEVkQlFVY3NUVUZCVFN4RFFVRkRMRWxCUVVrc1EwRkJReXhIUVVGSExFVkJRVVVzUzBGQlN5eERRVUZETEVOQlFVTTdiMEpCUTNCRExGZEJRVmNzUlVGQlJTeERRVUZETzI5Q1FVTmtMRTlCUVU4c1IwRkJSeXhEUVVGRE8yZENRVU5pTEVOQlFVTTdZVUZEUml4RFFVRkRPMWxCUTBZc2IwSkJRVzlDTEVOQlEyeENMRTFCUVUwc1JVRkRUaXhSUVVGUkxFVkJRMUlzUzBGQlN5eEZRVU5NTEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGQlF5eEpRVUZKTEVWQlFVVXNVVUZCVVN4RFFVRkRMRU5CUVVNc1IwRkJSeXhEUVVOd1JDeERRVUZETzFGQlEwb3NRMEZCUXl4RFFVRkRMRU5CUVVNN1NVRkRUQ3hEUVVGRExFTkJRVU1zUTBGQlF6dEJRVU5NTEVOQlFVTTdRVUZGUkN4VFFVRlRMSE5DUVVGelFpeERRVUZETEVWQlFVVXNUMEZCVHl4RlFVRkZMRmRCUVZjc1JVRkJSVHRKUVVOMFJDeE5RVUZOTEZGQlFWRXNSMEZCUnl4SlFVRkpMR2RDUVVGblFpeERRVUZETEZkQlFWY3NRMEZCUXl4RFFVRkRPMGxCUTI1RUxFbEJRVWtzVTBGQlV5eEhRVUZITEV0QlFVc3NRMEZCUXp0SlFVTjBRaXhUUVVGVExFOUJRVTg3VVVGRFpDeEpRVUZKTEVOQlFVTXNVMEZCVXl4SlFVRkpMRTlCUVU4c1EwRkJReXhSUVVGUkxFVkJRVVVzUTBGQlF6dFpRVU51UXl4UlFVRlJMRU5CUVVNc1QwRkJUeXhEUVVGRExFOUJRVThzUTBGQlF5eFJRVUZSTEVWQlFVVXNSVUZCUlN4UFFVRlBMRVZCUVVVc1NVRkJTU3hGUVVGRkxGTkJRVk1zUlVGQlJTeEpRVUZKTEVWQlFVVXNRMEZCUXl4RFFVRkRPMWxCUTNaRkxGTkJRVk1zUjBGQlJ5eEpRVUZKTEVOQlFVTTdVVUZEYmtJc1EwRkJRenRKUVVOSUxFTkJRVU03U1VGRFJDeFBRVUZQTEVWQlFVVXNRMEZCUXp0SlFVTldMRTlCUVU4c1EwRkJReXhSUVVGUkxFTkJRVU1zWjBKQlFXZENMRU5CUVVNc2EwSkJRV3RDTEVWQlFVVTdVVUZEY0VRc1NVRkJTU3hUUVVGVExFVkJRVVVzUTBGQlF6dFpRVU5rTEZGQlFWRXNRMEZCUXl4VlFVRlZMRVZCUVVVc1EwRkJRenRaUVVOMFFpeFRRVUZUTEVkQlFVY3NTMEZCU3l4RFFVRkRPMUZCUTNCQ0xFTkJRVU03U1VGRFNDeERRVUZETEVOQlFVTXNRMEZCUXp0SlFVTklMRTlCUVU4c1QwRkJUeXhEUVVGRE8wRkJRMnBDTEVOQlFVTTdRVUZGUkN4VFFVRlRMRzlDUVVGdlFpeERRVUZETEVWQlF6VkNMRTlCUVU4c1JVRkRVQ3hoUVVGaExFVkJRMklzYjBKQlFXOUNMRVZCUTNCQ0xFOUJRVThzUlVGRFVDeFhRVUZYTEVkQlExbzdTVUZEUXl4TlFVRk5MSEZEUVVGeFF5eEhRVUZITEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGRE0wVXNZVUZCWVN4RFFVRkRMRmxCUVZrc1EwRkJReXhUUVVGVExFVkJRM0JETEU5QlFVOHNRMEZEVWl4RFFVRkRPMGxCUTBZc1RVRkJUU3gxUWtGQmRVSXNSMEZETTBJc2NVTkJRWEZETzFGQlEzSkRMRTFCUVUwc1EwRkJReXgzUWtGQmQwSXNRMEZCUXl4aFFVRmhMRU5CUVVNc1VVRkJVU3hEUVVGRExGTkJRVk1zUlVGQlJTeFBRVUZQTEVOQlFVTXNRMEZCUXp0SlFVTTNSU3hOUVVGTkxHRkJRV0VzUjBGQlJ5eDFRa0ZCZFVJc1EwRkJReXhMUVVGTExFTkJRVU03U1VGRGNFUXNiMEpCUVc5Q0xFTkJRMnhDTEhGRFFVRnhRenRSUVVOdVF5eERRVUZETEVOQlFVTXNZVUZCWVN4RFFVRkRMRmxCUVZrc1EwRkJReXhUUVVGVE8xRkJRM1JETEVOQlFVTXNRMEZCUXl4aFFVRmhMRU5CUVVNc1VVRkJVU3hEUVVGRExGTkJRVk1zUlVGRGNFTXNUMEZCVHl4RlFVTlFMRTlCUVU4c1JVRkRVQ3hUUVVGVExFdEJRVXNzUTBGQlF5eFBRVUZQTzFGQlEzQkNMRXRCUVVzc1NVRkJTU3hEUVVGRExFZEJRVWNzUTBGQlF5eEZRVUZGTEVOQlFVTXNSMEZCUnl4VFFVRlRMRU5CUVVNc1RVRkJUU3hGUVVGRkxFTkJRVU1zUjBGQlJ5eERRVUZETEVWQlFVVXNRMEZCUXl4SlFVRkpMRU5CUVVNc1JVRkJSU3hEUVVGRE8xbEJRM0JFTEUxQlFVMHNSMEZCUnl4SFFVRkhMRVZCUVVVc1IwRkJSeXhUUVVGVExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTTdXVUZET1VJc09FTkJRVGhETzFsQlF6bERMRTFCUVUwc1MwRkJTeXhIUVVOVUxFZEJRVWNzUTBGQlF5eExRVUZMTEVOQlFVTXNaMEpCUVdkQ0xFTkJRVU1zU1VGQlNTeERRVUZETEVkQlFVY3NRMEZCUXl4TFFVRkxMRU5CUVVNc1VVRkJVU3hEUVVGRE8yZENRVU5xUkN4RFFVRkRMRU5CUVVNc1EwRkJReXhIUVVGSExFTkJRVU03WjBKQlExQXNRMEZCUXl4RFFVRkRMRWRCUVVjc1EwRkJReXhMUVVGTExFTkJRVU1zVDBGQlR5eERRVUZETEVOQlFVTTdXVUZEZWtJc1RVRkJUU3hOUVVGTkxFZEJRVWNzUzBGQlN5eERRVUZETEUxQlFVMHNRMEZCUXp0WlFVTTFRaXhOUVVGTkxFOUJRVThzUjBGQlJ5eFBRVUZQTEVOQlFVTXNVVUZCVVN4RFFVRkRMRzlDUVVGdlFpeERRVUZETEZGQlFWRXNRMEZCUXl4RFFVRkRPMWxCUTJoRkxFdEJRVXNzU1VGQlNTeERRVUZETEVkQlFVY3NRMEZCUXl4RlFVRkZMRU5CUVVNc1IwRkJSeXhOUVVGTkxFVkJRVVVzUTBGQlF5eEpRVUZKTEVOQlFVTXNSVUZCUlN4RFFVRkRPMmRDUVVOdVF5eGhRVUZoTEVOQlFVTXNTVUZCU1N4RFFVRkRMRWxCUVVrc1JVRkJSU3hMUVVGTExFTkJRVU1zUTBGQlF5eERRVUZETEVOQlFVTXNRMEZCUXp0blFrRkRia01zVjBGQlZ5eEZRVUZGTEVOQlFVTTdaMEpCUTJRc1NVRkJTU3hQUVVGUExFTkJRVU1zVFVGQlRTeEpRVUZKTEU5QlFVOHNRMEZCUXl4UFFVRlBMRU5CUVVNc1RVRkJUU3hIUVVGSExFTkJRVU1zUTBGQlF5eERRVUZETEVkQlFVY3NSVUZCUlN4RFFVRkRPMjlDUVVOMFJDeFBRVUZQTEVWQlFVVXNRMEZCUXp0blFrRkRXaXhEUVVGRE8xbEJRMGdzUTBGQlF6dFJRVU5JTEVOQlFVTTdTVUZEU0N4RFFVRkRMRU5CUTBZc1EwRkJRenRKUVVWR0xFMUJRVTBzZFVOQlFYVkRMRWRCUXpORExFMUJRVTBzUTBGQlF5eDNRa0ZCZDBJc1EwRkROMElzWVVGQllTeERRVUZETEZsQlFWa3NRMEZCUXl4VFFVRlRMRVZCUTNCRExGTkJRVk1zUTBGRFZpeERRVUZETzBsQlEwb3NUVUZCVFN4NVFrRkJlVUlzUjBGRE4wSXNkVU5CUVhWRE8xRkJRM1pETEUxQlFVMHNRMEZCUXl4M1FrRkJkMElzUTBGRE4wSXNZVUZCWVN4RFFVRkRMRkZCUVZFc1EwRkJReXhUUVVGVExFVkJRMmhETEZOQlFWTXNRMEZEVml4RFFVRkRPMGxCUTBvc1RVRkJUU3hsUVVGbExFZEJRVWNzZVVKQlFYbENMRU5CUVVNc1MwRkJTeXhEUVVGRE8wbEJRM2hFTEc5Q1FVRnZRaXhEUVVOc1FpeDFRMEZCZFVNN1VVRkRja01zUTBGQlF5eERRVUZETEdGQlFXRXNRMEZCUXl4WlFVRlpMRU5CUVVNc1UwRkJVenRSUVVOMFF5eERRVUZETEVOQlFVTXNZVUZCWVN4RFFVRkRMRkZCUVZFc1EwRkJReXhUUVVGVExFVkJRM0JETEZOQlFWTXNSVUZEVkN4UFFVRlBMRVZCUTFBc1UwRkJVeXhQUVVGUExFTkJRVU1zVDBGQlR6dFJRVU4wUWl4TFFVRkxMRWxCUVVrc1EwRkJReXhIUVVGSExFTkJRVU1zUlVGQlJTeERRVUZETEVkQlFVY3NVMEZCVXl4RFFVRkRMRTFCUVUwc1JVRkJSU3hEUVVGRExFZEJRVWNzUTBGQlF5eEZRVUZGTEVOQlFVTXNTVUZCU1N4RFFVRkRMRVZCUVVVc1EwRkJRenRaUVVOd1JDeE5RVUZOTEVkQlFVY3NSMEZCUnl4RlFVRkZMRWRCUVVjc1UwRkJVeXhEUVVGRExFTkJRVU1zUTBGQlF5eERRVUZETzFsQlF6bENMRTFCUVUwc1MwRkJTeXhIUVVGSExFZEJRVWNzUTBGQlF5eExRVUZMTEVOQlFVTXNUMEZCVHl4RFFVRkRMRU5CUVVNN1dVRkRha01zVFVGQlRTeE5RVUZOTEVkQlFVY3NTMEZCU3l4RFFVRkRMRTFCUVUwc1EwRkJRenRaUVVNMVFpeE5RVUZOTEU5QlFVOHNSMEZCUnl4UFFVRlBMRU5CUVVNc1VVRkJVU3hEUVVGRExHOUNRVUZ2UWl4RFFVRkRMRkZCUVZFc1EwRkJReXhEUVVGRE8xbEJRMmhGTEV0QlFVc3NTVUZCU1N4RFFVRkRMRWRCUVVjc1EwRkJReXhGUVVGRkxFTkJRVU1zUjBGQlJ5eE5RVUZOTEVWQlFVVXNRMEZCUXl4SlFVRkpMRU5CUVVNc1JVRkJSU3hEUVVGRE8yZENRVU51UXl4aFFVRmhMRU5CUVVNc1NVRkJTU3hEUVVGRExFbEJRVWtzUlVGQlJTeExRVUZMTEVOQlFVTXNRMEZCUXl4RFFVRkRMRU5CUVVNc1EwRkJRenRuUWtGRGJrTXNWMEZCVnl4RlFVRkZMRU5CUVVNN1owSkJRMlFzU1VGQlNTeFBRVUZQTEVOQlFVTXNUVUZCVFN4SlFVRkpMRTlCUVU4c1EwRkJReXhQUVVGUExFTkJRVU1zVFVGQlRTeEhRVUZITEVOQlFVTXNRMEZCUXl4RFFVRkRMRWRCUVVjc1JVRkJSU3hEUVVGRE8yOUNRVU4wUkN4UFFVRlBMRVZCUVVVc1EwRkJRenRuUWtGRFdpeERRVUZETzFsQlEwZ3NRMEZCUXp0UlFVTklMRU5CUVVNN1VVRkRSQ3hsUVVGbExFTkJRVU1zU1VGQlNTeERRVUZETEVsQlFVa3NSVUZCUlN4RlFVRkZMRU5CUVVNc1EwRkJRenRKUVVOcVF5eERRVUZETEVOQlEwWXNRMEZCUXp0QlFVTktMRU5CUVVNN1FVRkZSQ3hUUVVGVExHbENRVUZwUWl4RFFVRkRMRVZCUTNwQ0xFOUJRVThzUlVGRFVDeGhRVUZoTEVWQlEySXNiMEpCUVc5Q0xFVkJRM0JDTEdOQlFXTXNSMEZEWmp0SlFVTkRMRTFCUVUwc2IwSkJRVzlDTEVkQlFVY3NUVUZCVFN4RFFVRkRMSGRDUVVGM1FpeERRVU14UkN4aFFVRmhMRVZCUTJJc1RVRkJUU3hEUVVOUUxFTkJRVU03U1VGRFJpeE5RVUZOTEZWQlFWVXNSMEZCUnl4dlFrRkJiMElzUTBGQlF5eExRVUZMTEVOQlFVTTdTVUZET1VNc1RVRkJUU3hYUVVGWExFZEJRVWNzVFVGQlRTeERRVUZETEhkQ1FVRjNRaXhEUVVGRExFOUJRVThzUlVGQlJTeFZRVUZWTEVOQlFVTXNRMEZCUXl4SFFVRkhMRU5CUVVNN1NVRkROMFVzYjBKQlFXOUNMRU5CUVVNc1lVRkJZU3hGUVVGRkxFMUJRVTBzUlVGQlJTeFBRVUZQTEVWQlFVVXNVMEZCVXl4SlFVRkpPMUZCUTJoRkxFMUJRVTBzVTBGQlV5eEhRVUZITEZOQlFWTXNRMEZCUXl4TlFVRk5PMWxCUTJoRExFTkJRVU1zUTBGQlF5eFZRVUZWTEVOQlFVTXNTVUZCU1N4RFFVRkRMRWxCUVVrc1JVRkJSU3hIUVVGSExGTkJRVk1zUTBGQlF6dFpRVU55UXl4RFFVRkRMRU5CUVVNc1ZVRkJWU3hEUVVGRExFbEJRVWtzUTBGQlF5eEpRVUZKTEVOQlFVTXNRMEZCUXp0UlFVTXhRaXhKUVVGSkxGTkJRVk1zUlVGQlJTeERRVUZETzFsQlEyUXNjVVZCUVhGRk8xbEJRM0pGTEhORFFVRnpRenRaUVVOMFF5eGpRVUZqTEVOQlFVTXNWMEZCVnl4RFFVRkRMRWxCUVVrc1EwRkJReXhUUVVGVExFTkJRVU1zUTBGQlF5eFhRVUZYTEVOQlFVTXNRMEZCUXp0UlFVTXhSQ3hEUVVGRE8xRkJRMFFzVDBGQlR5eFRRVUZUTEVOQlFVTTdTVUZEYmtJc1EwRkJReXhEUVVGRExFTkJRVU03UVVGRFRDeERRVUZESW4wPSJdLCJuYW1lcyI6W10sInNvdXJjZVJvb3QiOiIifQ==