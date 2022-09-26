# -*- coding: utf-8 -*-

from classes_general import GeneralTextProcessor 

gtp = GeneralTextProcessor()

def test_html_remove():
    test_html = '<p class="nord-text text-micro leading-normal font-medium">Last updated: January 31, 2022</p></div><p class="nord-text text-base leading-normal mb-6">This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services.</p>'
    res_html = 'Last updated: January 31, 2022This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services.'
    assert gtp.replace_html_tags(test_html) == res_html

def test_html_replace():
    test_html = '<p class="nord-text text-micro leading-normal font-medium">Last updated: January 31, 2022</p>'
    res_html = '-Last updated: January 31, 2022-'
    assert gtp.replace_html_tags(test_html, "-") == res_html

def test_remove_nonalphanumeric():
    test_str = "{Last updated: January 31, 2022}"   
    res_str = "Last updated: January 31, 2022"
    assert gtp.remove_nonalphanumeric(test_str) == res_str

def test_to_lower():
    test_str = "Last updated: January 31, 2022"   
    res_str = "last updated: january 31, 2022"
    assert gtp.to_lower(test_str) == res_str

def test_lang_detect():
    test_str = "This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services."
    lang, prec = gtp.guess_language(test_str)
    assert lang == "en"

def test_sent_tknz():
    test_str = 'This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services. All definitions and capitalized words used in this Privacy Policy are defined here or in our General Terms. By visiting our websites, by submitting your personal data to us, and by accessing, installing and/or using the Services, you confirm that you have read this Privacy Policy and agree to be bound by this Privacy Policy. If you disagree with the rules of this Privacy Policy, please do not use our Services.'
    res = ['This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services.','All definitions and capitalized words used in this Privacy Policy are defined here or in our General Terms.', 'By visiting our websites, by submitting your personal data to us, and by accessing, installing and/or using the Services, you confirm that you have read this Privacy Policy and agree to be bound by this Privacy Policy.','If you disagree with the rules of this Privacy Policy, please do not use our Services.']
    assert gtp.tknz_sent(test_str) == res

def test_tknz2():
    test_str = '<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy. Our privacy policy is applicable to The Atlantic, and The Atlantics affiliates and subsidiaries whose websites, mobile applications and other online services are directly linked (the Sites). The privacy policy describes the kinds of information we may gather during your visit to these Sites, how we use your information, when we might disclose your personally identifiable information, and how you can manage your information. <br> <br>'
    res = ['<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy.','Our privacy policy is applicable to The Atlantic, and The Atlantics affiliates and subsidiaries whose websites, mobile applications and other online services are directly linked (the Sites).', 'The privacy policy describes the kinds of information we may gather during your visit to these Sites, how we use your information, when we might disclose your personally identifiable information, and how you can manage your information.','<br> <br>']
    assert gtp.tknz_sent(test_str) == res

def test_strip():
    assert gtp.strip("[...]\n") == "[...]"

def test_clean_split():
    test_str = "If you do not wish to participate in our [...] advertising personalization programs, you can opt out by unchecking the appropriate cookies on our Cookie preferences page. We do not allow third parties to track or collect your Personal Information on our sites for their own advertising purposes without your consent.\n\nPlease note that you can disable all cookies from your browser settings. However, our websites require cookies to function and disabling all cookies will limit functionality of the site, including actions like adding products to a cart, etc.\n\nAt this time only customers accessing our Platform from EEU regions and from Canada can withdraw consent.\n[...]\nYou have the ability to opt out of the use of our Cookies. Please visit our Cookie Preference Page where you can manage your preferences for the types of Cookies listed above.\n\nShould you wish to turn off all Cookies, you may do so in your browser's settings.\n[...]\nWe will also provide an individual opt-out or opt-in choice before we share your data with third parties other than our agents, or before we use it for a purpose other than for which it was originally collected or subsequently authorized. To request to limit the use and disclosure of your personal information, please submit a written request to compliance@bhphotovideo.com."
    res = ["if you do not wish to participate in our advertising personalization programs, you can opt out by unchecking the appropriate cookies on our cookie preferences page.","we do not allow third parties to track or collect your personal information on our sites for their own advertising purposes without your consent.","please note that you can disable all cookies from your browser settings.","however, our websites require cookies to function and disabling all cookies will limit functionality of the site, including actions like adding products to a cart, etc.","at this time only customers accessing our platform from eeu regions and from canada can withdraw consent.","you have the ability to opt out of the use of our cookies.","please visit our cookie preference page where you can manage your preferences for the types of cookies listed above.","should you wish to turn off all cookies, you may do so in your browser's settings.","we will also provide an individual opt-out or opt-in choice before we share your data with third parties other than our agents, or before we use it for a purpose other than for which it was originally collected or subsequently authorized.","to request to limit the use and disclosure of your personal information, please submit a written request to compliance@bhphotovideo.com."]
    print(gtp.split_into_clean_sentences(test_str))
    assert gtp.split_into_clean_sentences(test_str) == res

def test_clean_split2():
    test_str = 'This document (“Privacy Policy”) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the Nord Services.\nAll definitions and capitalized words used in this Privacy Policy are defined here or in our General Terms. By visiting our websites, by submitting your personal data to us, and by accessing, installing and/or using the Services, you confirm that you have read this Privacy Policy and agree to be bound by this Privacy Policy.\n'
    res = ['this document (privacy policy) explains the privacy rules applicable to all information collected or submitted when you access, install, or use the nord services.','all definitions and capitalized words used in this privacy policy are defined here or in our general terms.', 'by visiting our websites, by submitting your personal data to us, and by accessing, installing and/or using the services, you confirm that you have read this privacy policy and agree to be bound by this privacy policy.']
    assert gtp.split_into_clean_sentences(test_str) == res

def test_clean_split3():
    test_str = "Last updated: January 31, 2022"   
    res_str = ["last updated: january 31, 2022"]
    assert gtp.split_into_clean_sentences(test_str) == res_str

def test_check_text_empty():
    test_input = ["<br> <br>", "    ", "<br>Test<br>", "<a> </a>", "<a>{~[~&%</a>", "<a>Test2</a>"]
    res = [True, True, False, True, True, False]
    assert [gtp.check_if_text_empty(test) for test in test_input] == res

def test_return_index():
    total_str = '<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy. Our privacy policy is applicable to The Atlantic, and The Atlantics affiliates and subsidiaries whose websites, mobile applications and other online services are directly linked (the Sites). The privacy policy describes the kinds of information we may gather during your visit to these Sites, how we use your information, when we might disclose your personally identifiable information, and how you can manage your information. <br> <br>'
    sub_str = '<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy.'
    res = (0,334)
    assert gtp.return_substr_location(sub_str, total_str) == res

def test_return_indeces():
    total_str = 'This is a test sentence'
    sub_strs = ['Th', 's ', 'test', 'ten']
    res = [(0,2), (3,5), (10,14), (18,21)]
    assert [gtp.return_substr_location(sub_str, total_str) for sub_str in sub_strs] == res

def test_return_indeces_applied():
    total_str = '<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy. Our privacy policy is applicable to The Atlantic, and The Atlantics affiliates and subsidiaries whose websites, mobile applications and other online services are directly linked (the Sites). The privacy policy describes the kinds of information we may gather during your visit to these Sites, how we use your information, when we might disclose your personally identifiable information, and how you can manage your information.'
    sub_strs = ['<strong> Privacy Policy </strong> <br> <br> <strong> Effective: January 1, 2015 </strong> <br> <br> At the Atlantic Monthly Group, Inc. ("The Atlantic"), we want you to enjoy and benefit from our websites and online services secure in the knowledge that we have implemented fair information practices designed to protect your privacy.','Our privacy policy is applicable to The Atlantic, and The Atlantics affiliates and subsidiaries whose websites, mobile applications and other online services are directly linked (the Sites).', 'The privacy policy describes the kinds of information we may gather during your visit to these Sites, how we use your information, when we might disclose your personally identifiable information, and how you can manage your information.']
    res = [(0,334), (335,525), (526,762)]
    assert [gtp.return_substr_location(sub_str, total_str) for sub_str in sub_strs] == res

def test_html_escape():
    test_str = "&lt;a href=&quot;somewhere.com&quot;&gt;This is a link to somewhere&lt;/a&gt;"
    res = '<a href="somewhere.com">This is a link to somewhere</a>'
    assert gtp.html_unescape(test_str) == res
